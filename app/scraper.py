import asyncio
import concurrent.futures
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
    Scrapes real CS2 odds from HLTV.org using a PERSISTENT browser session.

    HLTV's betting page requires JavaScript execution (the odds widget is
    rendered client-side). curl_cffi gets a 200 but the odds are not in
    the raw HTML — we need a real browser.

    Persistent browser strategy:
      First load  : ~60-120s  (browser launch + Cloudflare JS challenge)
      Subsequent  : ~5-10s   (page reload with cached CF cookies)

    Uses a single-threaded executor so Playwright's sync API stays
    thread-safe.
    """

    def __init__(self):
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="hltv_browser"
        )
        self._browser = None
        self._page = None
        self._stealth_ctx = None
        self._pw_ctx = None
        self._initialized = False

    # ──────────────────────────────────────────────────────────────────
    # PUBLIC ENTRY POINT
    # ──────────────────────────────────────────────────────────────────

    async def scrape_all_sites(self) -> List[Dict]:
        try:
            loop = asyncio.get_event_loop()
            if not self._initialized:
                print("[HLTV] Launching browser (first time — up to 2min on Railway)...")
                timeout = 130.0
            else:
                print("[HLTV] Refreshing page (~5-10s)...")
                timeout = 35.0

            data = await asyncio.wait_for(
                loop.run_in_executor(
                    self._executor,
                    self._init_and_scrape if not self._initialized else self._refresh_and_scrape,
                ),
                timeout=timeout,
            )

            if data:
                print(f"[HLTV] {len(data)} odds entries from "
                      f"{len(set(d['source'] for d in data))} bookmakers")
                return self.normalize_team_names(data)

            print("[HLTV] Page loaded but no odds parsed")

        except asyncio.TimeoutError:
            print(f"[HLTV] Timed out — resetting browser for next cycle")
            self._executor.submit(self._reset_browser_sync)
        except Exception as e:
            print(f"[HLTV] Error: {e}")
            self._executor.submit(self._reset_browser_sync)

        return []

    # ──────────────────────────────────────────────────────────────────
    # INIT  (runs once in executor thread)
    # ──────────────────────────────────────────────────────────────────

    def _init_and_scrape(self) -> List[Dict]:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
        import time

        self._stealth_ctx = Stealth().use_sync(sync_playwright())
        self._pw_ctx = self._stealth_ctx.__enter__()

        self._browser = self._pw_ctx.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-extensions",
                "--disable-background-networking",
                "--disable-default-apps",
                "--disable-sync",
                "--no-first-run",
                "--memory-pressure-off",
            ],
        )
        self._page = self._browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )

        print("[HLTV] Browser ready — loading page (Cloudflare may take 30-60s)...")
        self._page.goto(
            "https://www.hltv.org/betting/money",
            timeout=90000,          # 90s for the goto itself
            wait_until="domcontentloaded",
        )
        print("[HLTV] DOM loaded — waiting for odds widget JS to render...")
        time.sleep(10)              # give the betting widget JS time to run
        self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(3)

        self._initialized = True
        print("[HLTV] Persistent browser ready")
        return self._parse_page()

    # ──────────────────────────────────────────────────────────────────
    # REFRESH  (every subsequent call)
    # ──────────────────────────────────────────────────────────────────

    def _refresh_and_scrape(self) -> List[Dict]:
        import time
        self._page.reload(timeout=20000, wait_until="domcontentloaded")
        time.sleep(8)
        self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(2)
        return self._parse_page()

    # ──────────────────────────────────────────────────────────────────
    # PAGE PARSER
    # ──────────────────────────────────────────────────────────────────

    def _parse_page(self) -> List[Dict]:
        from bs4 import BeautifulSoup

        html = self._page.content()
        soup = BeautifulSoup(html, "html.parser")
        results = []

        # Debug: how many containers found
        containers = soup.find_all("div", class_="b-match-container")
        print(f"[HLTV] b-match-container count: {len(containers)}")
        if not containers:
            # Log a snippet so we can see what we actually got
            title = soup.title.get_text() if soup.title else "no title"
            print(f"[HLTV] Page title: {title!r}")

        for mc in containers:
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
                bm_key = next(
                    (
                        c.replace("b-list-odds-provider-", "")
                        for c in ca.get("class", [])
                        if c.startswith("b-list-odds-provider-")
                    ),
                    None,
                )
                if not bm_key:
                    continue

                bm_name = BOOKMAKER_NAMES.get(bm_key.lower(), bm_key.title())
                tag_a, tag_b = ca.find("a"), cb.find("a")
                if not tag_a or not tag_b:
                    continue

                try:
                    odds_a = round(float(tag_a.get_text(strip=True)), 2)
                    odds_b = round(float(tag_b.get_text(strip=True)), 2)
                except ValueError:
                    continue

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
    # BROWSER RESET
    # ──────────────────────────────────────────────────────────────────

    def _reset_browser_sync(self):
        self._initialized = False
        for obj, method in [
            (self._browser, "close"),
            (self._stealth_ctx, "__exit__"),
        ]:
            try:
                if obj:
                    if method == "__exit__":
                        obj.__exit__(None, None, None)
                    else:
                        getattr(obj, method)()
            except Exception:
                pass
        self._browser = None
        self._page = None
        self._pw_ctx = None
        self._stealth_ctx = None
        print("[HLTV] Browser reset — will re-init on next cycle")

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
