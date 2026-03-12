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
    Scrapes real CS2 odds from HLTV.org using curl_cffi to bypass
    Cloudflare without needing a full browser.

    curl_cffi mimics Chrome's exact TLS fingerprint — works on datacenter
    IPs (Railway, etc.) where headless Playwright gets blocked.

    Keeps a persistent session so Cloudflare cookies are reused across
    scrape cycles (~1-2s per cycle after the first request).

    No mock data — returns [] if scraping fails so the UI shows an honest
    "no data" state rather than fake numbers.
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
        """Real HLTV data only. Uses curl_cffi to bypass Cloudflare."""
        try:
            loop = asyncio.get_event_loop()
            data = await asyncio.wait_for(
                loop.run_in_executor(self._executor, self._fetch_and_parse),
                timeout=30.0,
            )

            if data:
                print(f"[HLTV] {len(data)} odds entries from "
                      f"{len(set(d['source'] for d in data))} bookmakers")
                return self.normalize_team_names(data)

            print("[HLTV] Page fetched but no odds parsed")

        except asyncio.TimeoutError:
            print("[HLTV] Request timed out — will retry next cycle")
            self._session = None  # reset session so next attempt gets fresh cookies
        except Exception as e:
            print(f"[HLTV] Error: {e} — will retry next cycle")
            self._session = None

        return []  # real empty — no mock

    # ──────────────────────────────────────────────────────────────────
    # HTTP FETCH  (runs in executor thread)
    # ──────────────────────────────────────────────────────────────────

    def _fetch_and_parse(self) -> List[Dict]:
        from curl_cffi import requests as cffi_requests

        if self._session is None:
            self._session = cffi_requests.Session(impersonate="chrome120")
            print("[HLTV] New curl_cffi session created")

        resp = self._session.get(
            "https://www.hltv.org/betting/money",
            timeout=20,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
            },
        )

        if resp.status_code != 200:
            print(f"[HLTV] HTTP {resp.status_code} — Cloudflare may have blocked request")
            self._session = None
            return []

        print(f"[HLTV] HTTP {resp.status_code} — parsing HTML ({len(resp.text)} chars)")
        return self._parse_html(resp.text)

    # ──────────────────────────────────────────────────────────────────
    # HTML PARSER
    # ──────────────────────────────────────────────────────────────────

    def _parse_html(self, html: str) -> List[Dict]:
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
