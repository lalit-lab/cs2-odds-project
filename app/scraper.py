import asyncio
import aiohttp
import random
import os
import re
from typing import List, Dict
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz
from dotenv import load_dotenv

load_dotenv()

# Fallback: Real CS2 teams currently active on the pro circuit
REAL_CS2_TEAMS = [
    "Natus Vincere", "FaZe Clan", "Team Vitality", "G2 Esports",
    "Team Liquid", "Astralis", "Cloud9", "MOUZ", "Heroic", "ENCE",
    "NIP", "Fnatic", "BIG", "OG", "paiN Gaming", "FURIA",
    "Virtus.pro", "Spirit", "Apeks", "9z Team"
]

FIXED_MATCHES = [
    ("Natus Vincere", "FaZe Clan"),
    ("Team Vitality", "G2 Esports"),
    ("Team Liquid", "MOUZ"),
    ("Heroic", "Astralis"),
    ("Cloud9", "NIP"),
    ("FURIA", "paiN Gaming"),
    ("Spirit", "Virtus.pro"),
    ("ENCE", "Fnatic"),
]

# Bookmaker display names mapped from HLTV CSS class suffixes
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
    def __init__(self):
        self.odds_api_key = os.getenv("ODDS_API_KEY", "")
        # On Railway/cloud, Playwright often hangs (no display server).
        # Set HLTV_SCRAPE=true in Railway env vars to enable it there.
        # Locally it always tries HLTV first.
        self.use_playwright = os.getenv("RAILWAY_ENVIRONMENT") is None or \
                              os.getenv("HLTV_SCRAPE", "false").lower() == "true"

    # ------------------------------------------------------------------
    # PUBLIC ENTRY POINT
    # ------------------------------------------------------------------

    async def scrape_all_sites(self) -> List[Dict]:
        """
        On local: scrapes real CS2 odds from HLTV via Playwright.
        On Railway/cloud: uses realistic mock data with real team names.
        Falls back to mock data if HLTV scraping fails or times out.
        """
        if self.use_playwright:
            try:
                data = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, self._scrape_hltv_sync),
                    timeout=40.0
                )
                if data:
                    print(f"[HLTV] Scraped {len(data)} entries from {len(set(d['source'] for d in data))} bookmakers")
                    return self.normalize_team_names(data)
                print("[HLTV] No data returned, using mock data")
            except asyncio.TimeoutError:
                print("[HLTV] Timed out after 40s, using mock data")
            except Exception as e:
                print(f"[HLTV] Failed: {e}, using mock data")
        else:
            print("[Scraper] Cloud mode — using realistic mock data")

        data = await self._generate_realistic_mock()
        return self.normalize_team_names(data)

    # ------------------------------------------------------------------
    # HLTV SCRAPER  (playwright-stealth to bypass Cloudflare)
    # ------------------------------------------------------------------

    def _scrape_hltv_sync(self) -> List[Dict]:
        """
        Synchronously scrape https://www.hltv.org/betting/money
        using Playwright with stealth mode to bypass Cloudflare.
        Returns list of odds entries with source, teams, and odds.
        """
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
        from bs4 import BeautifulSoup
        import time

        results = []

        with Stealth().use_sync(sync_playwright()) as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )
            page = browser.new_page(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
            )
            page.goto("https://www.hltv.org/betting/money", timeout=30000)
            # Scroll to load all bookmaker columns
            time.sleep(4)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)

            soup = BeautifulSoup(page.content(), "html.parser")
            browser.close()

        # Parse the comparison table
        # Structure: each b-match-container holds a bookmakerMatch table
        # Rows: teamrow × 2 (team A and team B)
        # Columns: bookmakerTeamBox + one cell per bookmaker (b-list-odds-provider-{name})
        match_containers = soup.find_all("div", class_="b-match-container")

        for mc in match_containers:
            table = mc.find("table", class_="bookmakerMatch")
            if not table:
                continue

            rows = table.find_all("tr", class_="teamrow")
            if len(rows) != 2:
                continue

            # Extract team names
            def get_team_name(row):
                box = row.find("td", class_="bookmakerTeamBox")
                if not box:
                    return None
                img = box.find("img")
                if img and img.get("alt"):
                    return img["alt"].strip()
                a_tag = box.find("a")
                return a_tag.get_text(strip=True) if a_tag else None

            team_a = get_team_name(rows[0])
            team_b = get_team_name(rows[1])
            if not team_a or not team_b:
                continue

            # Extract odds per bookmaker from each cell
            # Cell classes like: "odds b-list-odds b-list-odds-provider-ggbet"
            row_a_cells = rows[0].find_all("td", class_="odds")
            row_b_cells = rows[1].find_all("td", class_="odds")

            for cell_a, cell_b in zip(row_a_cells, row_b_cells):
                # Identify bookmaker from class
                classes = cell_a.get("class", [])
                bm_key = None
                for cls in classes:
                    if cls.startswith("b-list-odds-provider-"):
                        bm_key = cls.replace("b-list-odds-provider-", "")
                        break
                if not bm_key:
                    continue

                bm_name = BOOKMAKER_NAMES.get(bm_key.lower(), bm_key.title())

                # Extract odds values
                odds_a_tag = cell_a.find("a")
                odds_b_tag = cell_b.find("a")
                if not odds_a_tag or not odds_b_tag:
                    continue

                try:
                    odds_a = float(odds_a_tag.get_text(strip=True))
                    odds_b = float(odds_b_tag.get_text(strip=True))
                except ValueError:
                    continue

                results.append({
                    "source": bm_name,
                    "team_a": team_a,
                    "team_b": team_b,
                    "team_a_odds": round(odds_a, 2),
                    "team_b_odds": round(odds_b, 2),
                    "match_time": datetime.utcnow().isoformat(),
                })

        return results

    # ------------------------------------------------------------------
    # ENHANCED MOCK DATA  (fallback — realistic CS2 teams + bookmaker spread)
    # ------------------------------------------------------------------

    async def _generate_realistic_mock(self) -> List[Dict]:
        tasks = [self._mock_bookmaker(bm) for bm in BOOKMAKER_NAMES.values()]
        results = await asyncio.gather(*tasks)
        all_results = []
        for r in results:
            all_results.extend(r)
        return all_results

    async def _mock_bookmaker(self, bookmaker: str) -> List[Dict]:
        await asyncio.sleep(random.uniform(0.05, 0.3))
        entries = []
        for team_a, team_b in FIXED_MATCHES:
            base_prob_a = random.uniform(0.35, 0.65)
            base_prob_b = 1 - base_prob_a
            margin = random.uniform(0.04, 0.08)
            odds_a = round(1 / (base_prob_a * (1 + margin / 2)), 2)
            odds_b = round(1 / (base_prob_b * (1 + margin / 2)), 2)
            noise = random.uniform(-0.05, 0.05)
            odds_a = max(1.01, round(odds_a + noise, 2))
            odds_b = max(1.01, round(odds_b - noise * 0.5, 2))
            entries.append({
                "source": bookmaker,
                "team_a": team_a,
                "team_b": team_b,
                "team_a_odds": odds_a,
                "team_b_odds": odds_b,
                "match_time": (datetime.utcnow() + timedelta(hours=random.randint(1, 48))).isoformat(),
            })
        return entries

    # ------------------------------------------------------------------
    # TEAM NAME NORMALIZER
    # ------------------------------------------------------------------

    def normalize_team_names(self, odds_data: List[Dict]) -> List[Dict]:
        team_mappings = {
            "natus vincere": ["Na'Vi", "NAVI", "Natus Vincere", "NaVi"],
            "faze clan": ["FaZe", "Faze Clan", "FAZE"],
            "team liquid": ["Liquid", "Team Liquid", "TL"],
            "g2 esports": ["G2", "G2 Esports", "G2 eSports"],
            "team vitality": ["Vitality", "Team Vitality", "VIT"],
            "astralis": ["Astralis", "AST"],
            "cloud9": ["Cloud9", "C9"],
            "mouz": ["MOUZ", "mousesports", "Mouz"],
            "heroic": ["Heroic", "HER"],
            "ence": ["ENCE"],
            "nip": ["NIP", "Ninjas in Pyjamas", "Ninjas In Pyjamas"],
            "fnatic": ["Fnatic", "fnatic"],
            "spirit": ["Spirit", "Team Spirit"],
            "virtus.pro": ["Virtus.pro", "VP", "virtus pro"],
            "furia": ["FURIA", "Furia"],
            "the mongolz": ["The MongolZ", "MongolZ"],
            "legacy": ["Legacy", "Legacy (BR)"],
            "fut": ["FUT", "FUT Esports"],
            "bestia": ["BESTIA"],
            "9ine": ["9INE"],
            "m80": ["M80"],
        }

        def normalize_name(name: str) -> str:
            name_lower = name.lower().strip()
            for canonical, variants in team_mappings.items():
                if name_lower in [v.lower() for v in variants]:
                    return canonical.title()
                for variant in variants:
                    if fuzz.ratio(name_lower, variant.lower()) > 85:
                        return canonical.title()
            return name  # Keep original if no match found

        for odds in odds_data:
            odds["team_a"] = normalize_name(odds["team_a"])
            odds["team_b"] = normalize_name(odds["team_b"])

        return odds_data
