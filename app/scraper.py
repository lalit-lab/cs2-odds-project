import asyncio
import concurrent.futures
import re
import json
from typing import List, Dict, Optional
from datetime import datetime
from fuzzywuzzy import fuzz
from dotenv import load_dotenv

load_dotenv()

BOOKMAKER_NAMES = {
    "ggbet": "GGBet",
    "thunderpick": "Thunderpick",
    "1xbet": "1xBet",
    "betlabel": "BetLabel",
    "vulkan": "Vulkan Bet",
    "epicbet": "EpicBet",
    "roobet": "Roobet",
    "melbet": "Melbet",
    "n1bet": "N1 Bet",
    "housebets": "Housebets",
    "bet20": "Bet20",
    "betify": "Betify",
    "bcgame": "BC.Game",
    "vavada": "Vavada",
    "ybets": "YBets",
    "coldbet": "ColdBet",
    "2up": "2UP",
}

AJAX_URL = "https://bcwp.hltv.org/wp-admin/admin-ajax.php"


class OddsScraper:
    """
    Scrapes real CS2 odds from HLTV.org.

    HLTV's /betting/money page embeds a WordPress-based betting widget
    (bcwp.hltv.org). The odds are NOT in the initial HTML — they're
    fetched via WordPress admin-ajax.php AJAX calls.

    Strategy:
      1. Fetch hltv.org/betting/money to extract the bcb_security nonce
         and configUrl embedded in the page JS.
      2. Fetch configUrl to get the list of matches/blocks.
      3. For each block, call admin-ajax.php to get real odds data.

    Uses curl_cffi (Chrome TLS impersonation) for all requests so
    Cloudflare doesn't block us.
    """

    def __init__(self):
        self._session = None
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="hltv_http"
        )

    # ──────────────────────────────────────────────────────────────────
    # PUBLIC ENTRY POINT
    # ──────────────────────────────────────────────────────────────────

    async def scrape_all_sites(self) -> List[Dict]:
        try:
            loop = asyncio.get_event_loop()
            data = await asyncio.wait_for(
                loop.run_in_executor(self._executor, self._fetch_and_parse),
                timeout=40.0,
            )
            if data:
                print(f"[HLTV] {len(data)} odds entries from "
                      f"{len(set(d['source'] for d in data))} bookmakers")
                return self.normalize_team_names(data)
            print("[HLTV] No odds parsed this cycle")
        except asyncio.TimeoutError:
            print("[HLTV] Timed out — resetting session")
            self._session = None
        except Exception as e:
            print(f"[HLTV] Error: {e}")
            self._session = None
        return []

    # ──────────────────────────────────────────────────────────────────
    # MAIN FETCH LOGIC  (runs in executor thread)
    # ──────────────────────────────────────────────────────────────────

    def _fetch_and_parse(self) -> List[Dict]:
        from curl_cffi import requests as cffi_requests

        if self._session is None:
            self._session = cffi_requests.Session(impersonate="chrome120")
            print("[HLTV] New session created")

        # ── Step 1: fetch main page, extract nonce + configUrl ────────
        resp = self._session.get(
            "https://www.hltv.org/betting/money",
            timeout=20,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        if resp.status_code != 200:
            print(f"[HLTV] Main page HTTP {resp.status_code} — resetting session")
            self._session = None
            return []

        html = resp.text
        nonce, config_url = self._extract_widget_config(html)

        if not nonce or not config_url:
            print(f"[HLTV] Could not extract nonce/configUrl — nonce={nonce!r} configUrl={config_url!r}")
            print(f"[HLTV] Page snippet: {html[5000:5500]}")
            self._session = None
            return []

        print(f"[HLTV] nonce={nonce}  configUrl={config_url}")

        # ── Step 2: fetch widget config JSON ──────────────────────────
        cfg_resp = self._session.get(config_url, timeout=15)
        if cfg_resp.status_code != 200:
            print(f"[HLTV] configUrl HTTP {cfg_resp.status_code}")
            return []

        try:
            config = cfg_resp.json()
        except Exception as e:
            print(f"[HLTV] configUrl not valid JSON: {e}")
            print(f"[HLTV] configUrl raw: {cfg_resp.text[:500]}")
            return []

        # Log the first few key→value pairs so we can see the URL structure
        for k, v in list(config.items())[:6]:
            print(f"[HLTV] config[{k!r}] = {str(v)[:200]}")

        # ── Step 3: config values are URLs to JSON data files ─────────
        results = self._fetch_offers_data(config)
        return results

    # ──────────────────────────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────────────────────────

    def _extract_widget_config(self, html: str):
        """Pull bcb_security nonce and configUrl out of the page JS."""
        nonce = None
        config_url = None

        # blocksData variable in a <script> tag
        m = re.search(r'var\s+blocksData\s*=\s*(\{.*?\})\s*;', html, re.DOTALL)
        if m:
            try:
                raw = m.group(1).replace('\\/', '/')
                data = json.loads(raw)
                nonce = data.get("bcb_security") or data.get("security")
                config_url = data.get("configUrl") or data.get("config_url")
            except Exception:
                pass

        # Fallback: regex directly
        if not nonce:
            nm = re.search(r'"bcb_security"\s*:\s*"([^"]+)"', html)
            if nm:
                nonce = nm.group(1)
        if not config_url:
            cm = re.search(r'"configUrl"\s*:\s*"((?:[^"\\]|\\.)*)"', html)
            if cm:
                config_url = cm.group(1).replace('\\/', '/')

        return nonce, config_url

    def _fetch_offers_data(self, config: dict) -> List[Dict]:
        """
        The update_cache_config.json values are URLs to JSON data files.
        'syncOffersData' → the main odds/offers file.
        'syncOperatorsData' → bookmaker info.
        Fetch them and parse odds.
        """
        # Priority order: offers (match odds) > operators (bookmaker metadata)
        url_keys = ["syncOffersData", "syncOperatorsData"]
        # Also try region-specific alt-data (India, global)
        for region in ["IN", "RU", "DE", "BR", "TR", "ID"]:
            url_keys.append(f"bcblSyncAltData_{region}")

        for key in url_keys:
            val = config.get(key)
            if not val:
                continue
            # Values can be a URL string OR a dict with a url key
            url = None
            if isinstance(val, str) and val.startswith("http"):
                url = val
            elif isinstance(val, dict):
                url = val.get("url") or val.get("dataUrl") or val.get("file")

            if not url:
                print(f"[HLTV] {key} value is not a URL: {str(val)[:100]}")
                continue

            print(f"[HLTV] Fetching {key}: {url}")
            try:
                r = self._session.get(url, timeout=15)
                print(f"[HLTV] {key} → {r.status_code}, {len(r.text)} chars: {r.text[:400]}")
                if r.status_code == 200 and r.text.strip():
                    try:
                        data = r.json()
                        results = self._parse_offers_json(data, key)
                        if results:
                            print(f"[HLTV] Parsed {len(results)} odds from {key}")
                            return results
                    except Exception as e:
                        print(f"[HLTV] JSON parse error for {key}: {e}")
            except Exception as e:
                print(f"[HLTV] Fetch error for {key}: {e}")

        return []

    def _parse_offers_json(self, data, source_key: str) -> List[Dict]:
        """
        Parse the offers JSON — we log the shape so we can learn its structure.
        Handles common BCB plugin formats.
        """
        # Log the top-level structure so we can see what we're working with
        if isinstance(data, dict):
            print(f"[HLTV] offers keys: {list(data.keys())[:10]}")
        elif isinstance(data, list):
            print(f"[HLTV] offers is a list of {len(data)} items")
            if data and isinstance(data[0], dict):
                print(f"[HLTV] first item keys: {list(data[0].keys())[:10]}")
                print(f"[HLTV] first item sample: {str(data[0])[:300]}")

        results = []

        # Unwrap common wrapper shapes
        if isinstance(data, dict):
            # Try to find the list of matches/events/offers
            items = (
                data.get("offers") or data.get("matches") or data.get("events") or
                data.get("games") or data.get("data") or data.get("items") or
                data.get("results") or []
            )
            if not items:
                # Maybe data itself is a mapping of match_id → match_data
                items = list(data.values()) if data else []
        elif isinstance(data, list):
            items = data
        else:
            return []

        for item in items:
            if not isinstance(item, dict):
                continue

            # Try to extract team names
            team_a = (
                item.get("team1") or item.get("home_team") or item.get("teamA") or
                item.get("team1_name") or item.get("participant1") or
                (item.get("teams") or [{}])[0].get("name", "") if item.get("teams") else ""
            )
            team_b = (
                item.get("team2") or item.get("away_team") or item.get("teamB") or
                item.get("team2_name") or item.get("participant2") or
                (item.get("teams") or [{}, {}])[1].get("name", "") if item.get("teams") else ""
            )

            if not team_a or not team_b:
                continue

            # Try to extract bookmaker odds
            bookmakers = (
                item.get("bookmakers") or item.get("odds") or
                item.get("operators") or item.get("markets") or []
            )
            for bm in bookmakers:
                if not isinstance(bm, dict):
                    continue
                bm_name = (
                    bm.get("name") or bm.get("bookmaker") or
                    bm.get("operator") or bm.get("title") or ""
                )
                try:
                    odds_a = round(float(
                        bm.get("odds1") or bm.get("home") or bm.get("team1") or
                        bm.get("odd1") or bm.get("win1") or 0
                    ), 2)
                    odds_b = round(float(
                        bm.get("odds2") or bm.get("away") or bm.get("team2") or
                        bm.get("odd2") or bm.get("win2") or 0
                    ), 2)
                except (ValueError, TypeError):
                    continue
                if odds_a > 1.0 and odds_b > 1.0:
                    results.append({
                        "source": bm_name,
                        "team_a": str(team_a),
                        "team_b": str(team_b),
                        "team_a_odds": odds_a,
                        "team_b_odds": odds_b,
                        "match_time": datetime.utcnow().isoformat(),
                    })

        return results

    # ──────────────────────────────────────────────────────────────────
    # TEAM NAME NORMALIZER
    # ──────────────────────────────────────────────────────────────────

    def normalize_team_names(self, odds_data: List[Dict]) -> List[Dict]:
        team_mappings = {
            "natus vincere": ["Na'Vi", "NAVI", "Natus Vincere", "NaVi"],
            "faze clan":     ["FaZe", "Faze Clan", "FAZE"],
            "team liquid":   ["Liquid", "Team Liquid", "TL"],
            "g2 esports":    ["G2", "G2 Esports", "G2 eSports"],
            "team vitality": ["Vitality", "Team Vitality", "VIT"],
            "astralis":      ["Astralis", "AST"],
            "cloud9":        ["Cloud9", "C9"],
            "mouz":          ["MOUZ", "mousesports", "Mouz"],
            "heroic":        ["Heroic"],
            "ence":          ["ENCE"],
            "nip":           ["NIP", "Ninjas in Pyjamas"],
            "fnatic":        ["Fnatic"],
            "spirit":        ["Spirit", "Team Spirit"],
            "virtus.pro":    ["Virtus.pro", "VP"],
            "furia":         ["FURIA"],
            "the mongolz":   ["The MongolZ", "MongolZ"],
            "legacy":        ["Legacy"],
            "fut":           ["FUT", "FUT Esports"],
            "bestia":        ["BESTIA"],
            "9ine":          ["9INE"],
            "m80":           ["M80"],
        }

        def normalize(name: str) -> str:
            nl = name.lower().strip()
            for canonical, variants in team_mappings.items():
                if nl in [v.lower() for v in variants]:
                    return canonical.title()
                if any(fuzz.ratio(nl, v.lower()) > 85 for v in variants):
                    return canonical.title()
            return name

        for o in odds_data:
            o["team_a"] = normalize(o["team_a"])
            o["team_b"] = normalize(o["team_b"])
        return odds_data
