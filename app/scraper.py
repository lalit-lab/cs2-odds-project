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

        print(f"[HLTV] Config top-level keys: {list(config.keys())[:15]}")

        # ── Step 3: extract odds from config or via AJAX ──────────────
        results = self._extract_odds_from_config(config, nonce)
        if not results:
            # Config didn't have inline odds — try AJAX call
            results = self._fetch_odds_via_ajax(config, nonce)
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

    def _extract_odds_from_config(self, config: dict, nonce: str) -> List[Dict]:
        """
        Some widget configs include inline odds data.
        Try common key names.
        """
        results = []

        # Try to find match/event data at common keys
        matches = (
            config.get("matches") or
            config.get("events") or
            config.get("games") or
            config.get("data") or
            []
        )

        if not matches:
            print(f"[HLTV] Config has no inline matches. Keys: {list(config.keys())}")
            return []

        print(f"[HLTV] Config has {len(matches)} inline entries")
        # Log the first entry so we learn its shape
        if matches:
            print(f"[HLTV] First entry keys: {list(matches[0].keys()) if isinstance(matches[0], dict) else matches[0]}")

        for match in matches:
            if not isinstance(match, dict):
                continue
            team_a = match.get("team1") or match.get("home") or match.get("teamA") or ""
            team_b = match.get("team2") or match.get("away") or match.get("teamB") or ""
            if not team_a or not team_b:
                continue
            bookmakers = match.get("bookmakers") or match.get("odds") or []
            for bm in bookmakers:
                if not isinstance(bm, dict):
                    continue
                bm_name = bm.get("name") or bm.get("bookmaker") or ""
                try:
                    odds_a = round(float(bm.get("odds1") or bm.get("oddsHome") or bm.get("team1") or 0), 2)
                    odds_b = round(float(bm.get("odds2") or bm.get("oddsAway") or bm.get("team2") or 0), 2)
                except (ValueError, TypeError):
                    continue
                if odds_a and odds_b:
                    results.append({
                        "source": bm_name,
                        "team_a": team_a,
                        "team_b": team_b,
                        "team_a_odds": odds_a,
                        "team_b_odds": odds_b,
                        "match_time": datetime.utcnow().isoformat(),
                    })
        return results

    def _fetch_odds_via_ajax(self, config: dict, nonce: str) -> List[Dict]:
        """
        Call bcwp.hltv.org/wp-admin/admin-ajax.php to get odds.
        Try common WordPress action names used by betting widgets.
        """
        results = []

        # Common actions used by BCB (Betting Content Builder) plugins
        actions_to_try = [
            "bcb_get_events",
            "bcb_get_odds",
            "bcb_get_matches",
            "get_betting_data",
            "bcb_load_block",
        ]

        # Some configs embed the action name
        action = (
            config.get("action") or
            config.get("ajax_action") or
            config.get("wp_action")
        )
        if action:
            actions_to_try.insert(0, action)

        for act in actions_to_try:
            try:
                r = self._session.post(
                    AJAX_URL,
                    data={
                        "action": act,
                        "security": nonce,
                        "nonce": nonce,
                    },
                    timeout=10,
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": "https://www.hltv.org/betting/money",
                        "Origin": "https://www.hltv.org",
                    },
                )
                print(f"[HLTV AJAX] action={act} → {r.status_code}: {r.text[:300]}")
                if r.status_code == 200 and r.text.strip() not in ("-1", "0", ""):
                    try:
                        payload = r.json()
                        parsed = self._parse_ajax_response(payload)
                        if parsed:
                            print(f"[HLTV AJAX] action={act} returned {len(parsed)} odds entries")
                            results = parsed
                            break
                    except Exception:
                        pass
            except Exception as e:
                print(f"[HLTV AJAX] action={act} error: {e}")

        return results

    def _parse_ajax_response(self, payload) -> List[Dict]:
        """Parse whatever the AJAX endpoint returns."""
        results = []

        # payload could be a list of matches, or {"data": [...], "success": true}, etc.
        if isinstance(payload, dict):
            payload = (
                payload.get("data") or
                payload.get("matches") or
                payload.get("events") or
                []
            )

        if not isinstance(payload, list):
            print(f"[HLTV AJAX] Unexpected payload type: {type(payload)}")
            return []

        for match in payload:
            if not isinstance(match, dict):
                continue
            team_a = match.get("team1") or match.get("home") or match.get("teamA") or ""
            team_b = match.get("team2") or match.get("away") or match.get("teamB") or ""
            if not team_a or not team_b:
                continue
            for bm in (match.get("bookmakers") or match.get("odds") or []):
                if not isinstance(bm, dict):
                    continue
                bm_name = bm.get("name") or bm.get("bookmaker") or ""
                try:
                    odds_a = round(float(bm.get("odds1") or bm.get("oddsHome") or 0), 2)
                    odds_b = round(float(bm.get("odds2") or bm.get("oddsAway") or 0), 2)
                except (ValueError, TypeError):
                    continue
                if odds_a and odds_b:
                    results.append({
                        "source": bm_name,
                        "team_a": team_a,
                        "team_b": team_b,
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
