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
    Tries multiple sources for live CS2 odds, in priority order:
      1. Strafe.gg  — esports odds aggregator (might serve SSR HTML)
      2. OddsPortal — general aggregator with esports/CS2 section
      3. HLTV       — original target (needs JS, often blocked on Railway)

    Whichever source first returns valid odds wins for that cycle.
    Uses curl_cffi (Chrome TLS impersonation) to bypass Cloudflare.
    Returns [] on total failure — no mock data.
    """

    def __init__(self):
        self._session = None
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
                loop.run_in_executor(self._executor, self._try_all_sources),
                timeout=50.0,
            )
            if data:
                print(f"[SCRAPER] {len(data)} odds from "
                      f"{len(set(d['source'] for d in data))} bookmakers")
                return self.normalize_team_names(data)
        except asyncio.TimeoutError:
            print("[SCRAPER] Timeout — resetting session")
            self._session = None
        except Exception as e:
            print(f"[SCRAPER] Error: {e}")
            self._session = None
        return []

    # ──────────────────────────────────────────────────────────────────
    # TRY ALL SOURCES IN ORDER
    # ──────────────────────────────────────────────────────────────────

    def _try_all_sources(self) -> List[Dict]:
        from curl_cffi import requests as cffi_requests

        if self._session is None:
            self._session = cffi_requests.Session(impersonate="chrome120")

        # ── Source 1: Strafe.gg ───────────────────────────────────────
        results = self._try_strafe()
        if results:
            return results

        # ── Source 2: OddsPortal ──────────────────────────────────────
        results = self._try_oddsportal()
        if results:
            return results

        # ── Source 3: HLTV (original, often blocked on Railway) ───────
        results = self._try_hltv()
        if results:
            return results

        print("[SCRAPER] All sources returned no data")
        return []

    # ──────────────────────────────────────────────────────────────────
    # SOURCE 1: STRAFE.GG
    # ──────────────────────────────────────────────────────────────────

    def _try_strafe(self) -> List[Dict]:
        """
        Strafe.gg aggregates esports odds from multiple bookmakers.
        Try their CS2 odds page and their API endpoint.
        """
        # Try their internal API first (used by their SPA)
        for url in [
            "https://api.strafe.gg/v1/matches?game=csgo&status=upcoming&limit=20",
            "https://api.strafe.gg/v2/matches?sport=csgo",
            "https://strafe.gg/api/csgo/matches",
        ]:
            try:
                r = self._session.get(url, timeout=10,
                    headers={"Accept": "application/json",
                             "Referer": "https://strafe.gg/"})
                print(f"[STRAFE] {url.split('/')[-1]} → {r.status_code}, {len(r.text)} chars: {r.text[:300]}")
                if r.status_code == 200 and r.text.strip().startswith(("{", "[")):
                    data = r.json()
                    results = self._parse_strafe(data)
                    if results:
                        print(f"[STRAFE] Parsed {len(results)} odds")
                        return results
            except Exception as e:
                print(f"[STRAFE] {url}: {e}")

        # Try the HTML page
        try:
            r = self._session.get("https://strafe.gg/csgo", timeout=12,
                headers={"Accept": "text/html,*/*", "Referer": "https://strafe.gg/"})
            print(f"[STRAFE HTML] {r.status_code}, {len(r.text)} chars")
            if r.status_code == 200:
                results = self._parse_strafe_html(r.text)
                if results:
                    return results
                # Log clues
                for bm in ["ggbet", "thunderpick", "betway", "pinnacle"]:
                    if bm in r.text.lower():
                        idx = r.text.lower().index(bm)
                        print(f"[STRAFE HTML] '{bm}' found: {r.text[max(0,idx-80):idx+150]}")
                        break
                else:
                    print("[STRAFE HTML] No bookmaker names found")
        except Exception as e:
            print(f"[STRAFE HTML] {e}")

        return []

    def _parse_strafe(self, data) -> List[Dict]:
        results = []
        items = data if isinstance(data, list) else (
            data.get("data") or data.get("matches") or data.get("events") or []
        )
        for item in items:
            if not isinstance(item, dict):
                continue
            teams = item.get("teams") or item.get("competitors") or []
            if len(teams) < 2:
                continue
            team_a = teams[0].get("name") or teams[0].get("team", {}).get("name", "")
            team_b = teams[1].get("name") or teams[1].get("team", {}).get("name", "")
            if not team_a or not team_b:
                continue
            odds = item.get("odds") or item.get("bookmakers") or []
            for bm in odds:
                if not isinstance(bm, dict):
                    continue
                bm_name = bm.get("name") or bm.get("bookmaker") or ""
                try:
                    oa = round(float(bm.get("odds1") or bm.get("home") or 0), 2)
                    ob = round(float(bm.get("odds2") or bm.get("away") or 0), 2)
                except (ValueError, TypeError):
                    continue
                if oa > 1.0 and ob > 1.0:
                    results.append({
                        "source": bm_name,
                        "team_a": team_a,
                        "team_b": team_b,
                        "team_a_odds": oa,
                        "team_b_odds": ob,
                        "match_time": datetime.utcnow().isoformat(),
                    })
        return results

    def _parse_strafe_html(self, html: str) -> List[Dict]:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        # Strafe uses React — look for __NEXT_DATA__ or window.__data__ JSON
        for script in soup.find_all("script"):
            txt = script.get_text()
            if "__NEXT_DATA__" in txt or "initialData" in txt:
                try:
                    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.+?)</script>', html, re.DOTALL)
                    if m:
                        data = __import__("json").loads(m.group(1))
                        print(f"[STRAFE __NEXT_DATA__] top keys: {list(data.keys())[:5]}")
                except Exception as e:
                    print(f"[STRAFE __NEXT_DATA__] parse error: {e}")
        return []

    # ──────────────────────────────────────────────────────────────────
    # SOURCE 2: ODDSPORTAL
    # ──────────────────────────────────────────────────────────────────

    def _try_oddsportal(self) -> List[Dict]:
        """
        OddsPortal.com has a counter-strike section with odds from multiple
        bookmakers. Try their internal API endpoints.
        """
        for url in [
            "https://www.oddsportal.com/api/v2/sport/esports/matches/?sport=esports&category=counter-strike",
            "https://www.oddsportal.com/esports/counter-strike/",
        ]:
            try:
                r = self._session.get(url, timeout=12,
                    headers={
                        "Accept": "application/json, text/html",
                        "Referer": "https://www.oddsportal.com/",
                    })
                print(f"[ODDSPORTAL] {url.split('/')[-2]} → {r.status_code}, {len(r.text)} chars: {r.text[:300]}")
                if r.status_code == 200:
                    if r.text.strip().startswith(("{", "[")):
                        data = r.json()
                        results = self._parse_oddsportal(data)
                        if results:
                            return results
                    else:
                        results = self._parse_oddsportal_html(r.text)
                        if results:
                            return results
            except Exception as e:
                print(f"[ODDSPORTAL] {url}: {e}")
        return []

    def _parse_oddsportal(self, data) -> List[Dict]:
        results = []
        items = data if isinstance(data, list) else (
            data.get("data") or data.get("matches") or []
        )
        for item in items:
            if not isinstance(item, dict):
                continue
            team_a = item.get("home") or item.get("team1") or ""
            team_b = item.get("away") or item.get("team2") or ""
            if not team_a or not team_b:
                continue
            for bm, o1, o2 in self._extract_op_odds(item):
                results.append({
                    "source": bm,
                    "team_a": team_a,
                    "team_b": team_b,
                    "team_a_odds": o1,
                    "team_b_odds": o2,
                    "match_time": datetime.utcnow().isoformat(),
                })
        return results

    def _extract_op_odds(self, item):
        for bm in (item.get("odds") or item.get("bookmakers") or []):
            if not isinstance(bm, dict):
                continue
            try:
                o1 = round(float(bm.get("odds1") or bm.get("home") or 0), 2)
                o2 = round(float(bm.get("odds2") or bm.get("away") or 0), 2)
                if o1 > 1.0 and o2 > 1.0:
                    yield bm.get("name", ""), o1, o2
            except (ValueError, TypeError):
                pass

    def _parse_oddsportal_html(self, html: str) -> List[Dict]:
        # Check for embedded JSON
        m = re.search(r'window\.__data\s*=\s*({.+?})\s*;', html, re.DOTALL)
        if m:
            try:
                data = __import__("json").loads(m.group(1))
                print(f"[ODDSPORTAL HTML] window.__data keys: {list(data.keys())[:5]}")
            except Exception:
                pass
        # Check for bookmaker names as a basic health check
        found = any(bm in html.lower() for bm in ["betway", "pinnacle", "unibet", "betfair"])
        print(f"[ODDSPORTAL HTML] bookmaker names found: {found}")
        return []

    # ──────────────────────────────────────────────────────────────────
    # SOURCE 3: HLTV (kept as last resort)
    # ──────────────────────────────────────────────────────────────────

    def _try_hltv(self) -> List[Dict]:
        try:
            r = self._session.get(
                "https://www.hltv.org/betting/money",
                timeout=15,
                headers={"Accept": "text/html,*/*", "Accept-Language": "en-US,en;q=0.5"},
            )
            print(f"[HLTV] {r.status_code}, {len(r.text)} chars")
            if r.status_code == 200:
                return self._parse_hltv_html(r.text)
        except Exception as e:
            print(f"[HLTV] {e}")
        return []

    def _parse_hltv_html(self, html: str) -> List[Dict]:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        results = []
        for mc in soup.find_all("div", class_="b-match-container"):
            table = mc.find("table", class_="bookmakerMatch")
            if not table:
                continue
            rows = table.find_all("tr", class_="teamrow")
            if len(rows) != 2:
                continue

            def _team(row):
                box = row.find("td", class_="bookmakerTeamBox")
                if not box:
                    return None
                img = box.find("img")
                if img and img.get("alt"):
                    return img["alt"].strip()
                a = box.find("a")
                return a.get_text(strip=True) if a else None

            team_a, team_b = _team(rows[0]), _team(rows[1])
            if not team_a or not team_b:
                continue
            for ca, cb in zip(
                rows[0].find_all("td", class_="odds"),
                rows[1].find_all("td", class_="odds"),
            ):
                bm_key = next((
                    c.replace("b-list-odds-provider-", "")
                    for c in ca.get("class", [])
                    if c.startswith("b-list-odds-provider-")
                ), None)
                if not bm_key:
                    continue
                bm_name = BOOKMAKER_NAMES.get(bm_key.lower(), bm_key.title())
                tag_a, tag_b = ca.find("a"), cb.find("a")
                if not tag_a or not tag_b:
                    continue
                try:
                    oa = round(float(tag_a.get_text(strip=True)), 2)
                    ob = round(float(tag_b.get_text(strip=True)), 2)
                except ValueError:
                    continue
                results.append({
                    "source": bm_name,
                    "team_a": team_a,
                    "team_b": team_b,
                    "team_a_odds": oa,
                    "team_b_odds": ob,
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
