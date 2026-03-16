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

# ── Cookie file paths ────────────────────────────────────────────────────────
_BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COOKIES_FILE  = os.path.join(_BASE_DIR, "cookies", "hltv_cookies.json")
UA_FILE       = os.path.join(_BASE_DIR, "cookies", "hltv_useragent.txt")

BOOKMAKER_NAMES = [
    "GGBet", "Thunderpick", "1xBet", "Vulkan Bet", "Roobet",
    "Betify", "BC.Game", "EpicBet", "Vavada", "Housebets",
    "Melbet", "N1 Bet", "Bet20", "ColdBet", "BetLabel",
    "YBets", "2UP",
]

# Bookmaker-specific margin offsets (used as fallback in generated odds)
BM_MARGINS = {
    "GGBet":       0.04,
    "Thunderpick": 0.03,
    "1xBet":       0.05,
    "Vulkan Bet":  0.05,
    "Roobet":      0.04,
    "Betify":      0.06,
    "BC.Game":     0.04,
    "EpicBet":     0.05,
    "Vavada":      0.06,
    "Housebets":   0.05,
    "Melbet":      0.07,
    "N1 Bet":      0.06,
    "Bet20":       0.05,
    "ColdBet":     0.03,
    "BetLabel":    0.04,
    "YBets":       0.06,
    "2UP":         0.07,
}


def _load_cookies() -> Optional[Dict[str, str]]:
    """
    Load saved HLTV cookies from cookies/hltv_cookies.json.
    Returns a flat name→value dict, or None if file is missing / empty.
    """
    if not os.path.exists(COOKIES_FILE):
        return None
    try:
        with open(COOKIES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not data:          # empty list []
            return None
        # Cookie-Editor exports a list of objects with "name" and "value"
        cookies = {}
        for c in data:
            name  = c.get("name")  or c.get("Name")
            value = c.get("value") or c.get("Value") or ""
            if name:
                cookies[name] = value
        if not cookies:
            return None
        print(f"[COOKIES] Loaded {len(cookies)} cookies from file "
              f"(cf_clearance={'__cf_clearance' in cookies})")
        return cookies
    except Exception as e:
        print(f"[COOKIES] Failed to load: {e}")
        return None


def _load_useragent() -> str:
    """Load saved User-Agent from cookies/hltv_useragent.txt, or use a default."""
    default = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
               "AppleWebKit/537.36 (KHTML, like Gecko) "
               "Chrome/120.0.0.0 Safari/537.36")
    if not os.path.exists(UA_FILE):
        return default
    try:
        ua = open(UA_FILE, encoding="utf-8").read().strip()
        return ua if ua else default
    except Exception:
        return default


class OddsScraper:
    """
    Fetches CS2 match odds from HLTV /betting/money using saved browser cookies
    to bypass Cloudflare.

    Setup (one-time):
      1. Visit https://www.hltv.org/betting/money in Chrome and pass Cloudflare.
      2. Export cookies via Cookie-Editor extension → save as cookies/hltv_cookies.json
      3. Copy your User-Agent → save to cookies/hltv_useragent.txt
      4. Cookies last hours→days. When expired, repeat steps 1-3.

    Fallback chain:
      HLTV /betting/money (real odds, cookies required)
        → HLTV /matches + generated odds (real team names, mock odds)
        → OddsPortal
    """

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
                print(f"[SCRAPER] {len(data)} odds entries from "
                      f"{len(set(d['source'] for d in data))} bookmakers")
                return self.normalize_team_names(data)
            print("[SCRAPER] No data this cycle")
        except asyncio.TimeoutError:
            print("[SCRAPER] Timeout — resetting session")
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

        # ── Try 1: HLTV /betting/money with saved cookies (real odds) ──
        cookies = _load_cookies()
        if cookies:
            real_odds = self._fetch_hltv_betting_odds(cookies)
            if real_odds:
                print(f"[SCRAPER] Got REAL odds for {len(real_odds)} bookmaker-match pairs")
                return real_odds
            print("[SCRAPER] Cookie fetch returned nothing — cookies may be expired. "
                  "Re-export from browser and save to cookies/hltv_cookies.json")
        else:
            print("[SCRAPER] No cookies found — using fallback. "
                  "See cookies/README.md to set up one-time login.")

        # ── Try 2: HLTV /matches (real team names) + generated odds ───
        matches = self._fetch_hltv_matches(cookies)
        if not matches:
            matches = self._fetch_oddsportal_matches()
        if not matches:
            print("[SCRAPER] Could not fetch any matches")
            return []

        print(f"[SCRAPER] {len(matches)} matches → generating odds (no real odds available)")
        return self._generate_odds(matches)

    # ──────────────────────────────────────────────────────────────────
    # SOURCE 1: HLTV /betting/money — REAL bookmaker odds
    # ──────────────────────────────────────────────────────────────────

    def _fetch_hltv_betting_odds(self, cookies: Dict[str, str]) -> List[Dict]:
        """
        Scrape https://www.hltv.org/betting/money with saved browser cookies.
        Parses real odds offered by multiple bookmakers for each CS2 match.
        Returns a flat list of {source, team_a, team_b, team_a_odds, team_b_odds}.
        """
        from bs4 import BeautifulSoup

        ua = _load_useragent()

        try:
            r = self._session.get(
                "https://www.hltv.org/betting/money",
                cookies=cookies,
                timeout=20,
                headers={
                    "User-Agent":      ua,
                    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Referer":         "https://www.hltv.org/",
                    "Cache-Control":   "no-cache",
                },
            )
            print(f"[HLTV BETTING] HTTP {r.status_code}, {len(r.text)} chars")

            if r.status_code in (403, 503):
                print("[HLTV BETTING] Blocked — cookies expired or invalid. "
                      "Re-export cookies from your browser.")
                self._session = None
                return []

            if r.status_code != 200:
                print(f"[HLTV BETTING] Unexpected status {r.status_code}")
                return []

            # Check if Cloudflare challenge page returned
            if "cf-browser-verification" in r.text or "Checking your browser" in r.text:
                print("[HLTV BETTING] Cloudflare challenge page — cookies expired.")
                return []

            return self._parse_hltv_betting_page(r.text)

        except Exception as e:
            print(f"[HLTV BETTING] Error: {e}")
            return []

    def _parse_hltv_betting_page(self, html: str) -> List[Dict]:
        """
        Parse the HLTV /betting/money HTML to extract real bookmaker odds.
        HLTV's betting page lists matches with a column per bookmaker.
        Tries multiple selector strategies since HLTV occasionally changes HTML.
        """
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        results = []

        # ── Strategy A: Standard betting match rows ───────────────────
        # HLTV structure: .betting-match-wrapper or .match-odds-row
        match_containers = (
            soup.find_all("div", class_=re.compile(r"betting-match", re.I))
            or soup.find_all("div", class_=re.compile(r"match-odds",  re.I))
            or soup.find_all("div", class_=re.compile(r"odds-row",    re.I))
        )

        if match_containers:
            print(f"[HLTV BETTING] Strategy A: {len(match_containers)} match containers")
            for mc in match_containers:
                parsed = self._parse_match_container(mc)
                results.extend(parsed)

        # ── Strategy B: Table-based layout ───────────────────────────
        if not results:
            tables = soup.find_all("table", class_=re.compile(r"bet|odds|match", re.I))
            for tbl in tables:
                rows = tbl.find_all("tr")
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 3:
                        parsed = self._parse_table_row(cells)
                        results.extend(parsed)
            if results:
                print(f"[HLTV BETTING] Strategy B (table): {len(results)} entries")

        # ── Strategy C: JSON embedded in page (Next.js / React hydration) ──
        if not results:
            results = self._extract_json_odds(html)
            if results:
                print(f"[HLTV BETTING] Strategy C (JSON): {len(results)} entries")

        # ── Strategy D: Regex on raw HTML ─────────────────────────────
        if not results:
            results = self._regex_extract_odds(html)
            if results:
                print(f"[HLTV BETTING] Strategy D (regex): {len(results)} entries")

        print(f"[HLTV BETTING] Parsed {len(results)} bookmaker-match odds entries")
        return results

    def _parse_match_container(self, container) -> List[Dict]:
        """Extract odds from a single match div block."""
        results = []

        # Find team names
        team_tags = container.find_all(
            class_=re.compile(r"team|opponent|matchTeam", re.I)
        )
        team_names = [
            t.get_text(strip=True) for t in team_tags
            if 2 < len(t.get_text(strip=True)) < 40
            and t.get_text(strip=True).lower() != "tbd"
        ]
        if len(team_names) < 2:
            return []
        team_a, team_b = team_names[0], team_names[1]

        # Find bookmaker blocks within this match container
        bm_blocks = container.find_all(
            class_=re.compile(r"bookmaker|bm-item|bet-item|odds-cell", re.I)
        )

        if bm_blocks:
            for bm in bm_blocks:
                bm_name_tag = bm.find(class_=re.compile(r"name|title|logo", re.I))
                bm_name = bm_name_tag.get_text(strip=True) if bm_name_tag else ""
                if not bm_name:
                    img = bm.find("img")
                    bm_name = (img.get("alt") or img.get("title") or "") if img else ""

                odds_tags = bm.find_all(class_=re.compile(r"odd|price|coeff", re.I))
                if not odds_tags:
                    odds_tags = bm.find_all(re.compile(r"span|div|td"))

                nums = []
                for tag in odds_tags:
                    txt = tag.get_text(strip=True).replace(",", ".")
                    try:
                        v = float(txt)
                        if 1.01 <= v <= 50.0:
                            nums.append(v)
                    except ValueError:
                        pass

                if len(nums) >= 2 and bm_name:
                    results.append({
                        "source":      bm_name,
                        "team_a":      team_a,
                        "team_b":      team_b,
                        "team_a_odds": nums[0],
                        "team_b_odds": nums[1],
                        "match_time":  datetime.utcnow().isoformat(),
                        "real_odds":   True,
                    })
        else:
            # No bm blocks — try to extract all odds numbers + guess sources
            all_nums = []
            for tag in container.find_all(re.compile(r"span|div|td|a")):
                txt = tag.get_text(strip=True).replace(",", ".")
                try:
                    v = float(txt)
                    if 1.01 <= v <= 50.0:
                        all_nums.append(v)
                except ValueError:
                    pass
            # pair up (every 2 numbers = one bookmaker)
            for i, bm_name in enumerate(BOOKMAKER_NAMES):
                idx = i * 2
                if idx + 1 < len(all_nums):
                    results.append({
                        "source":      bm_name,
                        "team_a":      team_a,
                        "team_b":      team_b,
                        "team_a_odds": all_nums[idx],
                        "team_b_odds": all_nums[idx + 1],
                        "match_time":  datetime.utcnow().isoformat(),
                        "real_odds":   True,
                    })

        return results

    def _parse_table_row(self, cells) -> List[Dict]:
        """Extract odds from a table row (fallback parser)."""
        texts = [c.get_text(strip=True) for c in cells]
        nums  = []
        for t in texts:
            try:
                v = float(t.replace(",", "."))
                if 1.01 <= v <= 50.0:
                    nums.append(v)
            except ValueError:
                pass
        if len(nums) < 2:
            return []
        # Can't reliably identify bookmaker from table row — use Unknown
        return [{
            "source":      "HLTV",
            "team_a":      texts[0] if texts else "Team A",
            "team_b":      texts[1] if len(texts) > 1 else "Team B",
            "team_a_odds": nums[0],
            "team_b_odds": nums[1],
            "match_time":  datetime.utcnow().isoformat(),
            "real_odds":   True,
        }]

    def _extract_json_odds(self, html: str) -> List[Dict]:
        """Try to extract odds from any JSON blob embedded in the page."""
        results = []
        # Look for any JSON array/object containing odds-shaped data
        for m in re.finditer(r'(\{[^{}]{20,}\})', html):
            try:
                obj = json.loads(m.group(1))
                team_a = (obj.get("team1") or obj.get("teamA") or
                          obj.get("home")  or obj.get("homeTeam") or "")
                team_b = (obj.get("team2") or obj.get("teamB") or
                          obj.get("away")  or obj.get("awayTeam") or "")
                odds_a = obj.get("odds1") or obj.get("oddsA") or obj.get("homeOdds")
                odds_b = obj.get("odds2") or obj.get("oddsB") or obj.get("awayOdds")
                bm     = obj.get("bookmaker") or obj.get("source") or "HLTV"
                if team_a and team_b and odds_a and odds_b:
                    try:
                        results.append({
                            "source":      str(bm),
                            "team_a":      str(team_a),
                            "team_b":      str(team_b),
                            "team_a_odds": float(odds_a),
                            "team_b_odds": float(odds_b),
                            "match_time":  datetime.utcnow().isoformat(),
                            "real_odds":   True,
                        })
                    except (ValueError, TypeError):
                        pass
            except (json.JSONDecodeError, AttributeError):
                pass
        return results

    def _regex_extract_odds(self, html: str) -> List[Dict]:
        """Last-resort: find team names + decimal odds via regex in raw HTML."""
        results = []

        # Pattern: team name tag followed closely by 2 decimal numbers
        pattern = re.compile(
            r'(?:team|opponent)[^>]*>([A-Za-z0-9 \.\-\']+)<.{0,200}?'
            r'(\d+\.\d{2}).{0,50}(\d+\.\d{2})',
            re.DOTALL | re.IGNORECASE
        )
        for m in pattern.finditer(html):
            team_a = m.group(1).strip()
            try:
                oa = float(m.group(2))
                ob = float(m.group(3))
                if 1.01 <= oa <= 30 and 1.01 <= ob <= 30:
                    results.append({
                        "source":      "HLTV",
                        "team_a":      team_a,
                        "team_b":      "Opponent",
                        "team_a_odds": oa,
                        "team_b_odds": ob,
                        "match_time":  datetime.utcnow().isoformat(),
                        "real_odds":   True,
                    })
            except ValueError:
                pass
        return results[:30]

    # ──────────────────────────────────────────────────────────────────
    # SOURCE 2: HLTV /matches — real team names (for fallback)
    # ──────────────────────────────────────────────────────────────────

    def _fetch_hltv_matches(self, cookies: Optional[Dict] = None) -> List[Dict]:
        """Scrape HLTV /matches for real team names (used when betting page fails)."""
        from bs4 import BeautifulSoup
        ua = _load_useragent()
        try:
            r = self._session.get(
                "https://www.hltv.org/matches",
                cookies=cookies or {},
                timeout=15,
                headers={
                    "User-Agent":      ua,
                    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate, br",
                },
            )
            print(f"[HLTV /matches] HTTP {r.status_code}, {len(r.text)} chars")
            if r.status_code != 200:
                return []

            soup = BeautifulSoup(r.text, "html.parser")
            matches = []

            for sel in [
                ("div", "upcomingMatch"),
                ("div", "match"),
                ("a",   "match-info-box"),
            ]:
                items = soup.find_all(sel[0], class_=sel[1])
                if items:
                    print(f"[HLTV /matches] {len(items)} items (class={sel[1]!r})")
                    for item in items[:30]:
                        teams = item.find_all(class_=re.compile(r"team|opponent|matchTeam", re.I))
                        names = [t.get_text(strip=True) for t in teams if t.get_text(strip=True)]
                        names = [n for n in names if 2 < len(n) < 40 and n.lower() != "tbd"]
                        if len(names) >= 2:
                            matches.append({"team_a": names[0], "team_b": names[1]})
                    if matches:
                        break

            if not matches:
                team_pattern = re.findall(
                    r'class="[^"]*(?:team|opponent)[^"]*"[^>]*>([^<]{2,35})<', r.text
                )
                seen = []
                for t in team_pattern:
                    t = t.strip()
                    if t and t.lower() not in ("tbd", "team") and t not in seen:
                        seen.append(t)
                matches = [
                    {"team_a": seen[i], "team_b": seen[i+1]}
                    for i in range(0, len(seen)-1, 2)
                    if seen[i] != seen[i+1]
                ][:20]

            print(f"[HLTV /matches] {len(matches)} matches extracted")
            return matches[:20]

        except Exception as e:
            print(f"[HLTV /matches] Error: {e}")
            return []

    # ──────────────────────────────────────────────────────────────────
    # SOURCE 3: OddsPortal (fallback)
    # ──────────────────────────────────────────────────────────────────

    def _fetch_oddsportal_matches(self) -> List[Dict]:
        try:
            r = self._session.get(
                "https://www.oddsportal.com/esports/counter-strike/",
                timeout=15,
                headers={"Accept": "text/html,*/*", "Referer": "https://www.oddsportal.com/"},
            )
            print(f"[ODDSPORTAL] HTTP {r.status_code}, {len(r.text)} chars")
            if r.status_code != 200:
                return []
            html = r.text
            m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
                          html, re.DOTALL)
            if m:
                try:
                    nd = json.loads(m.group(1))
                    matches = self._extract_op_matches(nd)
                    if matches:
                        return matches
                except Exception as e:
                    print(f"[ODDSPORTAL] parse error: {e}")
            return []
        except Exception as e:
            print(f"[ODDSPORTAL] Error: {e}")
            return []

    def _extract_op_matches(self, data, depth=0) -> List[Dict]:
        if depth > 6 or not isinstance(data, (dict, list)):
            return []
        matches = []
        if isinstance(data, dict):
            home = data.get("home-name") or data.get("home") or data.get("homeTeam")
            away = data.get("away-name") or data.get("away") or data.get("awayTeam")
            if home and away and isinstance(home, str) and isinstance(away, str):
                return [{"team_a": home.strip(), "team_b": away.strip()}]
            for v in data.values():
                matches.extend(self._extract_op_matches(v, depth + 1))
        elif isinstance(data, list):
            for item in data[:50]:
                matches.extend(self._extract_op_matches(item, depth + 1))
        return matches[:20]

    # ──────────────────────────────────────────────────────────────────
    # FALLBACK: Generated odds (when no real odds available)
    # ──────────────────────────────────────────────────────────────────

    def _generate_odds(self, matches: List[Dict]) -> List[Dict]:
        """
        Deterministic mock odds — used ONLY when real odds are unavailable.
        Each bookmaker applies its own margin so natural arbitrage appears.
        """
        results = []
        for match in matches:
            team_a = match["team_a"]
            team_b = match["team_b"]
            seed = int(hashlib.md5(
                f"{team_a.lower()}{team_b.lower()}".encode()
            ).hexdigest()[:8], 16)
            prob_a = 0.35 + (seed % 1000) / 3333.0

            for bm_name in BOOKMAKER_NAMES:
                margin = BM_MARGINS[bm_name]
                fair_a = 1.0 / prob_a
                fair_b = 1.0 / (1.0 - prob_a)
                odds_a = round(fair_a * (1.0 - margin * 0.5), 2)
                odds_b = round(fair_b * (1.0 - margin * 0.5), 2)
                odds_a = max(1.01, min(15.0, odds_a))
                odds_b = max(1.01, min(15.0, odds_b))
                results.append({
                    "source":      bm_name,
                    "team_a":      team_a,
                    "team_b":      team_b,
                    "team_a_odds": odds_a,
                    "team_b_odds": odds_b,
                    "match_time":  datetime.utcnow().isoformat(),
                    "real_odds":   False,   # flag so UI can show "mock" warning
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
