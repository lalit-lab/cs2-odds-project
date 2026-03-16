import asyncio
import concurrent.futures
import re
import json
import hashlib
import os
from typing import List, Dict, Optional
from datetime import datetime
from fuzzywuzzy import fuzz
from dotenv import load_dotenv

load_dotenv()

_BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COOKIES_FILE = os.path.join(_BASE_DIR, "cookies", "hltv_cookies.json")
UA_FILE      = os.path.join(_BASE_DIR, "cookies", "hltv_useragent.txt")

BOOKMAKER_NAMES = [
    "GGBet", "Thunderpick", "1xBet", "Vulkan Bet", "Roobet",
    "Betify", "BC.Game", "EpicBet", "Vavada", "Housebets",
    "Melbet", "N1 Bet", "Bet20", "ColdBet", "BetLabel",
    "YBets", "2UP",
]

BM_MARGINS = {
    "GGBet": 0.04, "Thunderpick": 0.03, "1xBet": 0.05, "Vulkan Bet": 0.05,
    "Roobet": 0.04, "Betify": 0.06, "BC.Game": 0.04, "EpicBet": 0.05,
    "Vavada": 0.06, "Housebets": 0.05, "Melbet": 0.07, "N1 Bet": 0.06,
    "Bet20": 0.05, "ColdBet": 0.03, "BetLabel": 0.04, "YBets": 0.06, "2UP": 0.07,
}

BOOKMAKER_URLS = {
    "GGBet": "https://ggbet.com", "Thunderpick": "https://thunderpick.io",
    "1xBet": "https://1xbet.com", "Vulkan Bet": "https://vulkanbet.com",
    "Roobet": "https://roobet.com", "Betify": "https://betify.com",
    "BC.Game": "https://bc.game", "EpicBet": "https://epicbet.com",
    "Vavada": "https://vavada.com", "Housebets": "https://housebets.com",
    "Melbet": "https://melbet.com", "N1 Bet": "https://n1bet.com",
    "Bet20": "https://bet20.com", "ColdBet": "https://coldbet.com",
    "BetLabel": "https://betlabel.com", "YBets": "https://ybets.com",
    "2UP": "https://2up.io",
}


def _load_useragent() -> str:
    default = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
               "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    ua = os.environ.get("HLTV_USERAGENT", "").strip()
    if ua:
        return ua
    try:
        ua = open(UA_FILE, encoding="utf-8").read().strip()
        return ua if ua else default
    except Exception:
        return default


class OddsScraper:

    def __init__(self):
        self._session  = None
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="odds_http"
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
                print(f"[SCRAPER] {len(data)} odds entries, "
                      f"{len(set(d['source'] for d in data))} bookmakers")
                return self.normalize_team_names(data)
            print("[SCRAPER] No data this cycle")
        except asyncio.TimeoutError:
            print("[SCRAPER] Timeout")
            self._session = None
        except Exception as e:
            print(f"[SCRAPER] Error: {e}")
            self._session = None
        return []

    # ──────────────────────────────────────────────────────────────────
    # MAIN FETCH
    # ──────────────────────────────────────────────────────────────────

    def _fetch_and_parse(self) -> List[Dict]:
        from curl_cffi import requests as cffi_requests
        if self._session is None:
            self._session = cffi_requests.Session(impersonate="chrome120")

        matches = self._fetch_oddsportal_matches()
        if not matches:
            print("[SCRAPER] OddsPortal returned nothing — using hardcoded fallback teams")
            matches = self._hardcoded_fallback()

        print(f"[SCRAPER] {len(matches)} CS2 matches → generating mock odds")
        return self._generate_odds(matches)

    # ──────────────────────────────────────────────────────────────────
    # SOURCE: OddsPortal — CS2 team names
    # ──────────────────────────────────────────────────────────────────

    def _fetch_oddsportal_matches(self) -> List[Dict]:
        from curl_cffi import requests as cffi_requests
        from bs4 import BeautifulSoup
        if self._session is None:
            self._session = cffi_requests.Session(impersonate="chrome120")
        try:
            r = self._session.get(
                "https://www.oddsportal.com/esports/counter-strike/",
                timeout=20,
                headers={
                    "User-Agent":      _load_useragent(),
                    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer":         "https://www.oddsportal.com/",
                },
            )
            print(f"[ODDSPORTAL] HTTP {r.status_code}, {len(r.text)} chars")
            if r.status_code != 200:
                return []

            html = r.text
            matches = []

            # Strategy A: __NEXT_DATA__ JSON
            m = re.search(
                r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
                html, re.DOTALL
            )
            if m:
                try:
                    nd = json.loads(m.group(1))
                    matches = self._extract_op_matches(nd)
                    if matches:
                        print(f"[ODDSPORTAL] {len(matches)} matches (NEXT_DATA)")
                        return matches
                except Exception:
                    pass

            # Strategy B: inline JSON blobs
            for js_m in re.finditer(r'({[^<]{30,}})', html):
                try:
                    obj = json.loads(js_m.group(1))
                    home = obj.get("home-name") or obj.get("homeName") or obj.get("home")
                    away = obj.get("away-name") or obj.get("awayName") or obj.get("away")
                    if home and away and isinstance(home, str) and isinstance(away, str):
                        if 2 < len(home) < 40 and 2 < len(away) < 40:
                            matches.append({"team_a": home.strip(), "team_b": away.strip()})
                except Exception:
                    pass
            if matches:
                print(f"[ODDSPORTAL] {len(matches)} matches (inline JSON)")
                return matches[:20]

            # Strategy C: BeautifulSoup links
            soup = BeautifulSoup(html, "html.parser")
            seen = set()
            for row in soup.find_all(["a", "div"],
                                     href=re.compile(r"/esports/counter-strike/.+/.+")):
                text = row.get_text(" ", strip=True)
                parts = re.split(r'\s*[-–vs]+\s*', text, maxsplit=1)
                if len(parts) == 2:
                    ta = re.sub(r'\s+\d.*', '', parts[0]).strip()
                    tb = re.sub(r'\s+\d.*', '', parts[1]).strip()
                    k = f"{ta}|{tb}"
                    if 2 < len(ta) < 40 and 2 < len(tb) < 40 and k not in seen:
                        seen.add(k)
                        matches.append({"team_a": ta, "team_b": tb})
            if matches:
                print(f"[ODDSPORTAL] {len(matches)} matches (links)")
                return matches[:20]

            # Strategy D: regex "TeamA - TeamB"
            seen = set()
            for m2 in re.finditer(
                r'([A-Z][A-Za-z0-9 \.\-\']{2,25})\s*[-–]\s*([A-Z][A-Za-z0-9 \.\-\']{2,25})',
                html
            ):
                ta, tb = m2.group(1).strip(), m2.group(2).strip()
                k = f"{ta}|{tb}"
                if ta != tb and k not in seen:
                    seen.add(k)
                    matches.append({"team_a": ta, "team_b": tb})
            if matches:
                print(f"[ODDSPORTAL] {len(matches)} matches (regex)")
                return matches[:20]

            return []

        except Exception as e:
            print(f"[ODDSPORTAL] Error: {e}")
            return []

    def _extract_op_matches(self, data, depth=0) -> List[Dict]:
        if depth > 8 or not isinstance(data, (dict, list)):
            return []
        matches = []
        if isinstance(data, dict):
            home = (data.get("home-name") or data.get("homeName") or
                    data.get("home") or data.get("homeTeam") or
                    data.get("team1") or data.get("teamOne"))
            away = (data.get("away-name") or data.get("awayName") or
                    data.get("away") or data.get("awayTeam") or
                    data.get("team2") or data.get("teamTwo"))
            if home and away and isinstance(home, str) and isinstance(away, str):
                if 2 < len(home) < 40 and 2 < len(away) < 40:
                    return [{"team_a": home.strip(), "team_b": away.strip()}]
            for v in data.values():
                matches.extend(self._extract_op_matches(v, depth + 1))
        elif isinstance(data, list):
            for item in data[:100]:
                matches.extend(self._extract_op_matches(item, depth + 1))
        return matches[:20]

    # ──────────────────────────────────────────────────────────────────
    # HARDCODED FALLBACK — used only if OddsPortal fails
    # ──────────────────────────────────────────────────────────────────

    def _hardcoded_fallback(self) -> List[Dict]:
        return [
            {"team_a": "Team Vitality",  "team_b": "Natus Vincere"},
            {"team_a": "FaZe Clan",      "team_b": "G2 Esports"},
            {"team_a": "Team Liquid",    "team_b": "MOUZ"},
            {"team_a": "Heroic",         "team_b": "ENCE"},
            {"team_a": "Astralis",       "team_b": "Cloud9"},
            {"team_a": "Team Spirit",    "team_b": "Virtus.pro"},
            {"team_a": "FURIA",          "team_b": "NIP"},
            {"team_a": "The MongolZ",    "team_b": "fnatic"},
        ]

    # ──────────────────────────────────────────────────────────────────
    # MOCK ODDS GENERATOR
    # ──────────────────────────────────────────────────────────────────

    def _generate_odds(self, matches: List[Dict]) -> List[Dict]:
        results = []
        for match in matches:
            team_a = match["team_a"]
            team_b = match["team_b"]
            seed = int(hashlib.md5(
                f"{team_a.lower()}{team_b.lower()}".encode()
            ).hexdigest()[:8], 16)
            base_prob_a = 0.35 + (seed % 1000) / 3333.0

            for bm_name in BOOKMAKER_NAMES:
                margin = BM_MARGINS[bm_name]
                bm_seed = int(hashlib.md5(
                    f"{team_a.lower()}{team_b.lower()}{bm_name}".encode()
                ).hexdigest()[:4], 16)
                prob_shift = ((bm_seed % 800) - 400) / 10000.0
                prob_a = max(0.05, min(0.95, base_prob_a + prob_shift))
                fair_a = 1.0 / prob_a
                fair_b = 1.0 / (1.0 - prob_a)
                odds_a = round(max(1.01, min(15.0, fair_a * (1.0 - margin * 0.5))), 2)
                odds_b = round(max(1.01, min(15.0, fair_b * (1.0 - margin * 0.5))), 2)
                results.append({
                    "source":      bm_name,
                    "team_a":      team_a,
                    "team_b":      team_b,
                    "team_a_odds": odds_a,
                    "team_b_odds": odds_b,
                    "match_time":  datetime.utcnow().isoformat(),
                    "real_odds":   False,
                })
        return results

    # ──────────────────────────────────────────────────────────────────
    # TEAM NAME NORMALIZER
    # ──────────────────────────────────────────────────────────────────

    def normalize_team_names(self, odds_data: List[Dict]) -> List[Dict]:
        team_mappings = {
            "natus vincere": ["Na'Vi", "NAVI", "Natus Vincere", "NaVi"],
            "faze clan":     ["FaZe", "Faze Clan", "FAZE", "FaZe Clan"],
            "team liquid":   ["Liquid", "Team Liquid", "TL"],
            "g2 esports":    ["G2", "G2 Esports", "G2 eSports"],
            "team vitality": ["Vitality", "Team Vitality", "VIT"],
            "astralis":      ["Astralis", "AST"],
            "cloud9":        ["Cloud9", "C9"],
            "mouz":          ["MOUZ", "mousesports", "Mouz"],
            "heroic":        ["Heroic"],
            "ence":          ["ENCE"],
            "nip":           ["NIP", "Ninjas in Pyjamas"],
            "fnatic":        ["Fnatic", "fnatic"],
            "spirit":        ["Spirit", "Team Spirit"],
            "virtus.pro":    ["Virtus.pro", "VP"],
            "furia":         ["FURIA"],
            "the mongolz":   ["The MongolZ", "MongolZ"],
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
