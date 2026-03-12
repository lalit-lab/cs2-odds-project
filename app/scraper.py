import asyncio
import concurrent.futures
import re
from typing import List, Dict
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


class OddsScraper:
    """
    Scrapes real CS2 odds from HLTV.org.

    Strategy: curl_cffi bypasses Cloudflare for HLTV's main page (200 OK).
    The odds widget JS files on bcwp.hltv.org are NOT behind Cloudflare,
    so we can fetch them directly to discover the exact data file URLs.
    """

    def __init__(self):
        self._session = None
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="hltv_http"
        )
        # Cache the resolved data URL so we don't re-parse JS every cycle
        self._data_url: str = ""
        self._data_url_base: str = ""   # base path (without timestamp)

    # ──────────────────────────────────────────────────────────────────
    # PUBLIC ENTRY POINT
    # ──────────────────────────────────────────────────────────────────

    async def scrape_all_sites(self) -> List[Dict]:
        try:
            loop = asyncio.get_event_loop()
            data = await asyncio.wait_for(
                loop.run_in_executor(self._executor, self._fetch_and_parse),
                timeout=45.0,
            )
            if data:
                print(f"[HLTV] {len(data)} odds entries from "
                      f"{len(set(d['source'] for d in data))} bookmakers")
                return self.normalize_team_names(data)
            print("[HLTV] No odds this cycle")
        except asyncio.TimeoutError:
            print("[HLTV] Timeout — resetting session")
            self._session = None
        except Exception as e:
            print(f"[HLTV] Error: {e}")
            self._session = None
        return []

    # ──────────────────────────────────────────────────────────────────
    # MAIN FETCH  (executor thread)
    # ──────────────────────────────────────────────────────────────────

    def _fetch_and_parse(self) -> List[Dict]:
        from curl_cffi import requests as cffi_requests
        from bs4 import BeautifulSoup

        if self._session is None:
            self._session = cffi_requests.Session(impersonate="chrome120")

        # ── Step 1: get HLTV page (bypasses Cloudflare via TLS impersonation) ─
        resp = self._session.get(
            "https://www.hltv.org/betting/money",
            timeout=20,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
            },
        )
        if resp.status_code != 200:
            print(f"[HLTV] Main page HTTP {resp.status_code}")
            self._session = None
            return []

        html = resp.text

        # ── Step 2: extract bcwp nonce + configUrl ────────────────────
        nonce = self._extract_nonce(html)

        # ── Step 3: if we don't know the data URL yet, read the JS to find it ─
        if not self._data_url_base:
            self._discover_data_url(html)

        if not self._data_url_base:
            print("[HLTV] Could not discover data URL from JS — dumping script srcs")
            soup = BeautifulSoup(html, "html.parser")
            srcs = [s.get("src", "") for s in soup.find_all("script") if s.get("src")]
            for s in srcs:
                print(f"[HLTV]   script src: {s}")
            return []

        # ── Step 4: fetch the actual data JSON ────────────────────────
        return self._fetch_data_json(nonce)

    # ──────────────────────────────────────────────────────────────────
    # DISCOVER DATA URL FROM JS
    # ──────────────────────────────────────────────────────────────────

    def _discover_data_url(self, html: str):
        """
        Multiple strategies to find where bc-sports-blocks fetches odds data from.
        """
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")

        # ── Strategy A: read the big bc-sports-blocks JS ─────────────
        bcwp_scripts = [
            s["src"] for s in soup.find_all("script", src=True)
            if "bcwp.hltv.org" in s.get("src", "")
            or "assets-bcwp.hltv.org" in s.get("src", "")
        ]
        for js_url in bcwp_scripts:
            try:
                r = self._session.get(js_url, timeout=10)
                if r.status_code != 200:
                    continue
                js = r.text

                # Broad search: any string containing "bc-blocks" or "uploads"
                for term in ["bc-blocks-data", "uploads/bc", "syncOffers",
                             "offersData", "getData", "getOffers"]:
                    if term in js:
                        idx = js.index(term)
                        ctx = js[max(0, idx - 120):idx + 250]
                        print(f"[HLTV JS] '{term}' in {js_url.split('/')[-1].split('?')[0]}: {ctx}")

                # Any https://bcwp URL
                urls = re.findall(r'https?://(?:assets-)?bcwp\.hltv\.org[^\s\'"\\,)]+', js)
                if urls:
                    print(f"[HLTV JS] bcwp URLs: {urls[:8]}")
                    for u in urls:
                        if any(k in u for k in ["offers", "data", "sync", "json"]):
                            self._data_url_base = u.split("?")[0]
                            print(f"[HLTV] Data URL found in JS: {self._data_url_base}")
                            return

                # fetch/XHR patterns
                fetches = re.findall(r'fetch\(([^)]{5,80})\)', js)
                if fetches:
                    print(f"[HLTV JS] fetch() calls: {fetches[:5]}")

            except Exception as e:
                print(f"[HLTV JS] error {js_url}: {e}")

        # ── Strategy B: try directory listing ─────────────────────────
        try:
            r = self._session.get(
                "http://bcwp.hltv.org/wp-content/uploads/bc-blocks-data/",
                timeout=8
            )
            print(f"[HLTV DIR] status={r.status_code}, size={len(r.text)}: {r.text[:600]}")
        except Exception as e:
            print(f"[HLTV DIR] error: {e}")

        # ── Strategy C: bchltv/v1 REST endpoints ─────────────────────
        for path in [
            "wp-json/bchltv/v1/pages",     # only known route — get page list
            "wp-json/bchltv/v1/pages/1",   # first page content
            "wp-json/bchltv/v1/pages/2",
            "wp-json/",
        ]:
            try:
                r = self._session.get(
                    f"https://bcwp.hltv.org/{path}", timeout=8
                )
                print(f"[HLTV REST] {path} → {r.status_code}: {r.text[:600]}")
                if r.status_code == 200 and ('"offers"' in r.text or '"matches"' in r.text
                                              or '"events"' in r.text):
                    self._data_url_base = f"https://bcwp.hltv.org/{path}"
                    print(f"[HLTV] REST endpoint found: {self._data_url_base}")
                    return
            except Exception as e:
                print(f"[HLTV REST] {path} error: {e}")

        # ── Strategy D-extra: try admin-ajax as GET (POST returned 400) ─
        nonce_val = self._extract_nonce(html) if 'html' in dir() else ""
        for action in ["bcb_get_offers", "bcb_sync_offers", "bchltv_get_data",
                       "bcb_get_sports_data", "bcb_load_data"]:
            try:
                r = self._session.get(
                    f"https://bcwp.hltv.org/wp-admin/admin-ajax.php"
                    f"?action={action}&security={nonce_val}&nonce={nonce_val}",
                    timeout=8,
                    headers={"Referer": "https://www.hltv.org/betting/money"},
                )
                print(f"[HLTV AJAX-GET] {action} → {r.status_code}: {r.text[:200]}")
                if r.status_code == 200 and r.text.strip() not in ("0", "-1", ""):
                    try:
                        data = r.json()
                        results = self._parse_data_json(data)
                        if results:
                            self._data_url_base = f"GET:{action}"
                            print(f"[HLTV] AJAX GET action={action} worked!")
                            return
                    except Exception:
                        pass
            except Exception as e:
                print(f"[HLTV AJAX-GET] {action} error: {e}")

        # ── Strategy E: try plausible timestamp-based filenames ───────
        ts = 1773207330  # syncOffersData value
        for pattern in [
            f"http://bcwp.hltv.org/wp-content/uploads/bc-blocks-data/{ts}.json",
            f"http://bcwp.hltv.org/wp-content/uploads/bc-blocks-data/offers/{ts}.json",
            f"http://bcwp.hltv.org/wp-content/uploads/bc-blocks-data/syncOffersData/{ts}.json",
        ]:
            try:
                r = self._session.get(pattern, timeout=5)
                print(f"[HLTV TS] {pattern.split('/')[-2:][-1]} → {r.status_code}, {len(r.text)} chars")
                if r.status_code == 200 and r.text.strip().startswith(("{", "[")):
                    self._data_url_base = pattern
                    print(f"[HLTV] Timestamp URL works: {self._data_url_base}")
                    return
            except Exception:
                pass

    def _extract_nonce(self, html: str) -> str:
        m = re.search(r'"bcb_security"\s*:\s*"([^"]+)"', html)
        return m.group(1) if m else ""

    # ──────────────────────────────────────────────────────────────────
    # FETCH AND PARSE THE DATA JSON
    # ──────────────────────────────────────────────────────────────────

    def _fetch_data_json(self, nonce: str) -> List[Dict]:
        """Fetch the data JSON from the discovered URL and parse odds."""
        url = self._data_url_base
        print(f"[HLTV] Fetching data: {url}")
        try:
            r = self._session.get(url, timeout=15)
            print(f"[HLTV] Data → HTTP {r.status_code}, {len(r.text)} chars: {r.text[:500]}")
            if r.status_code == 200 and r.text.strip():
                data = r.json()
                results = self._parse_data_json(data)
                if results:
                    return results
                # If parse failed, clear cache so we re-discover next cycle
                print("[HLTV] JSON parse returned no results — clearing URL cache")
                self._data_url_base = ""
        except Exception as e:
            print(f"[HLTV] Data fetch error: {e}")
            self._data_url_base = ""
        return []

    def _parse_data_json(self, data) -> List[Dict]:
        results = []
        if isinstance(data, dict):
            items = (data.get("offers") or data.get("matches") or
                     data.get("events") or data.get("data") or
                     data.get("items") or list(data.values()))
        elif isinstance(data, list):
            items = data
        else:
            print(f"[HLTV] Unexpected JSON type: {type(data)}")
            return []

        if items and isinstance(items[0] if items else None, dict):
            print(f"[HLTV] JSON items: {len(items)}, first keys: {list(items[0].keys())[:8]}")

        for item in items:
            if not isinstance(item, dict):
                continue
            team_a = (item.get("team1") or item.get("home_team") or
                      item.get("teamA") or item.get("team1_name") or "")
            team_b = (item.get("team2") or item.get("away_team") or
                      item.get("teamB") or item.get("team2_name") or "")
            if not team_a or not team_b:
                continue
            for bm in (item.get("bookmakers") or item.get("odds") or
                       item.get("operators") or []):
                if not isinstance(bm, dict):
                    continue
                bm_name = (bm.get("name") or bm.get("bookmaker") or
                           bm.get("operator") or "")
                try:
                    odds_a = round(float(
                        bm.get("odds1") or bm.get("home") or bm.get("team1") or
                        bm.get("odd1") or bm.get("win1") or 0), 2)
                    odds_b = round(float(
                        bm.get("odds2") or bm.get("away") or bm.get("team2") or
                        bm.get("odd2") or bm.get("win2") or 0), 2)
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
