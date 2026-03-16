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


def _parse_cookie_list(data) -> Optional[Dict[str, str]]:
    """Convert a Cookie-Editor JSON list → flat name:value dict."""
    if not data:
        return None
    cookies = {}
    for c in data:
        name  = c.get("name")  or c.get("Name")
        value = c.get("value") or c.get("Value") or ""
        if name:
            cookies[name] = value
    if not cookies:
        return None
    print(f"[COOKIES] Loaded {len(cookies)} cookies "
          f"(cf_clearance={'__cf_clearance' in cookies})")
    return cookies


def _load_cookies() -> Optional[Dict[str, str]]:
    """
    Load HLTV cookies.  Priority:
      1. HLTV_COOKIES_JSON env var  (Railway / any cloud host)
      2. cookies/hltv_cookies.json  (local dev)
    Returns a flat name→value dict, or None if nothing is configured.
    """
    # ── 1. Environment variable (Railway) ────────────────────────────
    raw = os.environ.get("HLTV_COOKIES_JSON", "").strip()
    if raw:
        try:
            data = json.loads(raw)
            result = _parse_cookie_list(data)
            if result:
                print("[COOKIES] Source: HLTV_COOKIES_JSON env var")
                return result
        except Exception as e:
            print(f"[COOKIES] HLTV_COOKIES_JSON parse error: {e}")

    # ── 2. Local file ────────────────────────────────────────────────
    if not os.path.exists(COOKIES_FILE):
        return None
    try:
        with open(COOKIES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        result = _parse_cookie_list(data)
        if result:
            print("[COOKIES] Source: cookies/hltv_cookies.json file")
        return result
    except Exception as e:
        print(f"[COOKIES] Failed to load file: {e}")
        return None


def _load_useragent() -> str:
    """
    Load User-Agent.  Priority:
      1. HLTV_USERAGENT env var  (Railway)
      2. cookies/hltv_useragent.txt  (local dev)
      3. Built-in Chrome 120 default
    """
    default = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
               "AppleWebKit/537.36 (KHTML, like Gecko) "
               "Chrome/120.0.0.0 Safari/537.36")
    ua = os.environ.get("HLTV_USERAGENT", "").strip()
    if ua:
        return ua
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
        self._session       = None
        self._hltv_blocked  = False   # set True after first confirmed IP block
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

        cookies = _load_cookies()

        if not self._hltv_blocked:
            # ── Try 1: HLTV /betting/money with saved cookies ──────────
            if cookies:
                real_odds = self._fetch_hltv_betting_odds(cookies)
                if real_odds:
                    print(f"[SCRAPER] Got REAL odds for {len(real_odds)} bookmaker-match pairs")
                    return real_odds
                print("[SCRAPER] Cookie fetch returned nothing — cookies may be expired.")

            # ── Try 2: HLTV /betting/money without cookies (TLS only) ──
            print("[SCRAPER] Trying /betting/money without cookies (TLS impersonation)...")
            real_odds = self._fetch_hltv_betting_odds({})
            if real_odds:
                print(f"[SCRAPER] Got REAL odds via TLS impersonation ({len(real_odds)} entries)")
                return real_odds
        else:
            print("[SCRAPER] HLTV IP-blocked — skipping")

        # ── Try 3: The Odds API (real odds, free API key required) ─────
        real_odds = self._fetch_theoddsapi()
        if real_odds:
            print(f"[SCRAPER] Got REAL odds from The Odds API: {len(real_odds)} entries")
            return real_odds

        # ── Try 4: Strafe.gg (real CS2 odds, no key needed) ────────────
        real_odds = self._fetch_strafe()
        if real_odds:
            print(f"[SCRAPER] Got REAL odds from Strafe.gg: {len(real_odds)} entries")
            return real_odds

        # ── Try 5: OddsPortal team names + generated odds (fallback) ───
        print("[SCRAPER] All real-odds sources failed — using generated odds")
        matches = self._fetch_hltv_matches(cookies)
        if not matches:
            matches = self._fetch_oddsportal_matches()
        if not matches:
            print("[SCRAPER] Could not fetch any matches")
            return []

        print(f"[SCRAPER] {len(matches)} matches → generating odds")
        return self._generate_odds(matches)

    # ──────────────────────────────────────────────────────────────────
    # SOURCE A: The Odds API — real bookmaker odds via free API key
    # Sign up free at https://the-odds-api.com  (500 req/month free)
    # Set ODDS_API_KEY in Railway env vars to enable.
    # ──────────────────────────────────────────────────────────────────

    def _fetch_theoddsapi(self) -> List[Dict]:
        api_key = os.environ.get("ODDS_API_KEY", "").strip()
        if not api_key:
            return []

        import requests as std_requests  # use standard requests (no CF needed)

        # CS2 sport keys to try in order
        sport_keys = ["esports_cs2", "esports_csgo", "csgo", "esports"]
        results = []

        for sport_key in sport_keys:
            try:
                r = std_requests.get(
                    f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds/",
                    params={
                        "apiKey":      api_key,
                        "regions":     "eu,uk,us",
                        "markets":     "h2h",
                        "oddsFormat":  "decimal",
                    },
                    timeout=15,
                )
                print(f"[ODDS-API] {sport_key} → HTTP {r.status_code}")
                if r.status_code == 404:
                    continue   # sport key not found, try next
                if r.status_code == 401:
                    print("[ODDS-API] Invalid API key — check ODDS_API_KEY env var")
                    return []
                if r.status_code != 200:
                    continue

                data = r.json()
                if not data:
                    continue

                print(f"[ODDS-API] {len(data)} matches from sport '{sport_key}'")
                for match in data:
                    team_a = match.get("home_team", "")
                    team_b = match.get("away_team", "")
                    if not team_a or not team_b:
                        continue
                    for bm in match.get("bookmakers", []):
                        bm_name = bm.get("title") or bm.get("key", "Unknown")
                        for market in bm.get("markets", []):
                            if market.get("key") != "h2h":
                                continue
                            outcomes = market.get("outcomes", [])
                            price_map = {o["name"]: o["price"] for o in outcomes if "name" in o}
                            odds_a = price_map.get(team_a)
                            odds_b = price_map.get(team_b)
                            if odds_a and odds_b:
                                results.append({
                                    "source":      bm_name,
                                    "team_a":      team_a,
                                    "team_b":      team_b,
                                    "team_a_odds": float(odds_a),
                                    "team_b_odds": float(odds_b),
                                    "match_time":  match.get("commence_time",
                                                             datetime.utcnow().isoformat()),
                                    "real_odds":   True,
                                })
                if results:
                    return results
            except Exception as e:
                print(f"[ODDS-API] Error for {sport_key}: {e}")

        if not results:
            print("[ODDS-API] No CS2 matches found. "
                  "Visit https://the-odds-api.com/v4/sports/?apiKey=YOUR_KEY "
                  "to see available sport keys.")
        return results

    # ──────────────────────────────────────────────────────────────────
    # SOURCE B: Strafe.gg — CS2-specific odds aggregator (no key needed)
    # ──────────────────────────────────────────────────────────────────

    def _fetch_strafe(self) -> List[Dict]:
        from curl_cffi import requests as cffi_requests
        if self._session is None:
            self._session = cffi_requests.Session(impersonate="chrome120")

        results = []

        # Try Strafe.gg JSON API (used by their frontend)
        api_urls = [
            "https://strafe.gg/api/v2/matches?sport=csgo&status=upcoming&page=1",
            "https://strafe.gg/api/matches?sport=cs2&status=upcoming",
            "https://strafe.gg/api/v1/matches?sport=csgo",
        ]
        for url in api_urls:
            try:
                r = self._session.get(
                    url,
                    timeout=15,
                    headers={
                        "User-Agent":  _load_useragent(),
                        "Accept":      "application/json",
                        "Referer":     "https://strafe.gg/",
                    },
                )
                print(f"[STRAFE] {url.split('?')[0]} → HTTP {r.status_code}")
                if r.status_code != 200:
                    continue
                try:
                    data = r.json()
                except Exception:
                    continue
                matches_raw = (data if isinstance(data, list) else
                               data.get("data") or data.get("matches") or
                               data.get("result") or [])
                for m in matches_raw[:30]:
                    ta = (m.get("team1_name") or m.get("team1", {}).get("name") or
                          m.get("home", {}).get("name") or m.get("teamA") or "")
                    tb = (m.get("team2_name") or m.get("team2", {}).get("name") or
                          m.get("away", {}).get("name") or m.get("teamB") or "")
                    if not ta or not tb:
                        continue
                    # Look for odds in the match object
                    odds_raw = (m.get("odds") or m.get("bookmakers") or
                                m.get("betting") or {})
                    if isinstance(odds_raw, dict):
                        for bm_name, bm_odds in odds_raw.items():
                            if isinstance(bm_odds, dict):
                                oa = bm_odds.get("1") or bm_odds.get("home") or bm_odds.get("team1")
                                ob = bm_odds.get("2") or bm_odds.get("away") or bm_odds.get("team2")
                            elif isinstance(bm_odds, list) and len(bm_odds) >= 2:
                                oa, ob = bm_odds[0], bm_odds[1]
                            else:
                                continue
                            try:
                                results.append({
                                    "source":      str(bm_name),
                                    "team_a":      ta,
                                    "team_b":      tb,
                                    "team_a_odds": float(oa),
                                    "team_b_odds": float(ob),
                                    "match_time":  m.get("date", datetime.utcnow().isoformat()),
                                    "real_odds":   True,
                                })
                            except (TypeError, ValueError):
                                pass
                if results:
                    print(f"[STRAFE] {len(results)} odds entries")
                    return results
            except Exception as e:
                print(f"[STRAFE] Error: {e}")

        # Fallback: scrape the HTML page for __NEXT_DATA__
        try:
            r = self._session.get(
                "https://strafe.gg/csgo",
                timeout=15,
                headers={"User-Agent": _load_useragent(), "Accept": "text/html,*/*"},
            )
            print(f"[STRAFE] HTML page → HTTP {r.status_code}, {len(r.text)} chars")
            if r.status_code == 200:
                m = re.search(
                    r'<script id="__NEXT_DATA__"[^>]*>(.+?)</script>',
                    r.text, re.DOTALL
                )
                if m:
                    nd = json.loads(m.group(1))
                    results = self._extract_strafe_next_data(nd)
                    if results:
                        print(f"[STRAFE] NEXT_DATA: {len(results)} entries")
                        return results
                print(f"[STRAFE] Snippet: {r.text[1000:1400]!r}")
        except Exception as e:
            print(f"[STRAFE] HTML error: {e}")

        return []

    def _extract_strafe_next_data(self, data, depth=0) -> List[Dict]:
        """Walk Strafe.gg's NEXT_DATA tree looking for match+odds objects."""
        if depth > 8 or not isinstance(data, (dict, list)):
            return []
        results = []
        if isinstance(data, dict):
            ta = (data.get("team1_name") or data.get("teamOneName") or
                  data.get("home_name") or "")
            tb = (data.get("team2_name") or data.get("teamTwoName") or
                  data.get("away_name") or "")
            odds = data.get("odds") or data.get("bookmakers") or {}
            if ta and tb and isinstance(odds, dict):
                for bm_name, bm_odds in odds.items():
                    try:
                        if isinstance(bm_odds, list) and len(bm_odds) >= 2:
                            results.append({
                                "source":      str(bm_name),
                                "team_a":      ta, "team_b": tb,
                                "team_a_odds": float(bm_odds[0]),
                                "team_b_odds": float(bm_odds[1]),
                                "match_time":  datetime.utcnow().isoformat(),
                                "real_odds":   True,
                            })
                    except (TypeError, ValueError):
                        pass
            for v in data.values():
                results.extend(self._extract_strafe_next_data(v, depth + 1))
        elif isinstance(data, list):
            for item in data[:50]:
                results.extend(self._extract_strafe_next_data(item, depth + 1))
        return results

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
                print("[HLTV BETTING] Blocked (403/503) — Cloudflare IP block on this server.")
                self._hltv_blocked = True
                return []

            if r.status_code != 200:
                print(f"[HLTV BETTING] Unexpected status {r.status_code}")
                return []

            # Check if Cloudflare challenge page returned
            if "cf-browser-verification" in r.text or "Checking your browser" in r.text:
                print("[HLTV BETTING] Cloudflare challenge page returned.")
                return []

            # Debug: log first 500 chars + all unique class names to help tune parser
            from bs4 import BeautifulSoup as _BS
            _soup = _BS(r.text, "html.parser")
            _classes = set()
            for tag in _soup.find_all(True):
                for c in (tag.get("class") or []):
                    _classes.add(c)
            _bet_classes = sorted(c for c in _classes if any(
                k in c.lower() for k in ("bet","odd","match","team","book","money")
            ))
            print(f"[HLTV BETTING] Page classes with bet/odd/match/team: {_bet_classes[:30]}")
            print(f"[HLTV BETTING] Page snippet: {r.text[2000:2500]!r}")

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
        from curl_cffi import requests as cffi_requests
        if self._session is None:
            self._session = cffi_requests.Session(impersonate="chrome120")
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

            # ── Strategy A: __NEXT_DATA__ JSON blob ───────────────────
            m = re.search(
                r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
                html, re.DOTALL
            )
            if m:
                try:
                    nd = json.loads(m.group(1))
                    matches = self._extract_op_matches(nd)
                    if matches:
                        print(f"[ODDSPORTAL] Strategy A (NEXT_DATA): {len(matches)} matches")
                        return matches
                except Exception as e:
                    print(f"[ODDSPORTAL] NEXT_DATA parse error: {e}")

            # ── Strategy B: any inline JSON with home/away team names ─
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
                print(f"[ODDSPORTAL] Strategy B (inline JSON): {len(matches)} matches")
                return matches[:20]

            # ── Strategy C: BeautifulSoup — event rows ─────────────────
            soup = BeautifulSoup(html, "html.parser")
            seen_pairs = set()
            for row in soup.find_all(["a", "div"], href=re.compile(r"/esports/counter-strike/.+/.+")):
                text = row.get_text(" ", strip=True)
                parts = re.split(r'\s*[-–vs]+\s*', text, maxsplit=1)
                if len(parts) == 2:
                    ta, tb = parts[0].strip(), parts[1].strip()
                    ta = re.sub(r'\s+\d.*', '', ta).strip()
                    tb = re.sub(r'\s+\d.*', '', tb).strip()
                    key = f"{ta}|{tb}"
                    if 2 < len(ta) < 40 and 2 < len(tb) < 40 and key not in seen_pairs:
                        seen_pairs.add(key)
                        matches.append({"team_a": ta, "team_b": tb})
            if matches:
                print(f"[ODDSPORTAL] Strategy C (links): {len(matches)} matches")
                return matches[:20]

            # ── Strategy D: regex — "TeamA - TeamB" patterns ──────────
            for m2 in re.finditer(
                r'([A-Z][A-Za-z0-9 \.\-\']{2,25})\s*[-–]\s*([A-Z][A-Za-z0-9 \.\-\']{2,25})',
                html
            ):
                ta, tb = m2.group(1).strip(), m2.group(2).strip()
                if ta != tb:
                    matches.append({"team_a": ta, "team_b": tb})
            if matches:
                # deduplicate
                seen = set()
                unique = []
                for m3 in matches:
                    k = f"{m3['team_a']}|{m3['team_b']}"
                    if k not in seen:
                        seen.add(k)
                        unique.append(m3)
                print(f"[ODDSPORTAL] Strategy D (regex): {len(unique)} matches")
                return unique[:20]

            # debug: print page snippet so we can see what's there
            print(f"[ODDSPORTAL] No matches found. Snippet: {html[1000:1500]!r}")
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
                    data.get("home")      or data.get("homeTeam") or
                    data.get("team1")     or data.get("teamOne"))
            away = (data.get("away-name") or data.get("awayName") or
                    data.get("away")      or data.get("awayTeam") or
                    data.get("team2")     or data.get("teamTwo"))
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
    # FALLBACK: Generated odds (when no real odds available)
    # ──────────────────────────────────────────────────────────────────

    def _generate_odds(self, matches: List[Dict]) -> List[Dict]:
        """
        Deterministic mock odds — used ONLY when real odds are unavailable.
        Each bookmaker has its own probability view (±4%) so the best odds
        across bookmakers occasionally create real arbitrage opportunities.
        """
        results = []
        for match in matches:
            team_a = match["team_a"]
            team_b = match["team_b"]

            # Base seed from team names — stable across cycles
            seed = int(hashlib.md5(
                f"{team_a.lower()}{team_b.lower()}".encode()
            ).hexdigest()[:8], 16)
            base_prob_a = 0.35 + (seed % 1000) / 3333.0

            for i, bm_name in enumerate(BOOKMAKER_NAMES):
                margin = BM_MARGINS[bm_name]

                # Each bookmaker has a unique probability view (±4% shift)
                # seeded on both match AND bookmaker so it's stable
                bm_seed = int(hashlib.md5(
                    f"{team_a.lower()}{team_b.lower()}{bm_name}".encode()
                ).hexdigest()[:4], 16)
                # shift in range [-0.04, +0.04]
                prob_shift = ((bm_seed % 800) - 400) / 10000.0
                prob_a = max(0.05, min(0.95, base_prob_a + prob_shift))

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
                    "real_odds":   False,
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
