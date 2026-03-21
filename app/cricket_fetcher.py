"""
Cricket odds fetcher using The Odds API (https://the-odds-api.com)
Fetches live h2h odds for all available cricket competitions.
"""
import os
import asyncio
import concurrent.futures
from typing import List, Dict
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Cricket sport keys supported by The Odds API
CRICKET_SPORT_KEYS = [
    "cricket_ipl",
    "cricket_big_bash",
    "cricket_caribbean_premier_league",
    "cricket_international_t20",
    "cricket_odi",
    "cricket_test_match",
    "cricket_psl",
    "cricket_sa20",
    "cricket_the_hundred",
    "cricket_icc_world_test_championship",
]

ODDS_API_BASE = "https://api.the-odds-api.com/v4"


class CricketFetcher:

    def __init__(self):
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="cricket_http"
        )
        self._api_key = os.getenv("ODDS_API_KEY", "")

    async def fetch_cricket_odds(self) -> Dict:
        """Fetch cricket odds from The Odds API. Returns dict with matches and metadata."""
        if not self._api_key:
            return {"matches": [], "error": "ODDS_API_KEY not configured", "requests_remaining": None}

        loop = asyncio.get_event_loop()
        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(self._executor, self._fetch_all_cricket),
                timeout=30.0,
            )
            return result
        except asyncio.TimeoutError:
            return {"matches": [], "error": "Request timed out", "requests_remaining": None}
        except Exception as e:
            return {"matches": [], "error": str(e), "requests_remaining": None}

    def _fetch_all_cricket(self) -> Dict:
        import requests

        all_matches = []
        requests_remaining = None
        requests_used = None

        # First: get list of in-season cricket sports
        try:
            r = requests.get(
                f"{ODDS_API_BASE}/sports",
                params={"apiKey": self._api_key},
                timeout=10,
            )
            requests_remaining = r.headers.get("x-requests-remaining")
            requests_used = r.headers.get("x-requests-used")

            if r.status_code == 200:
                sports = r.json()
                active_cricket = [
                    s["key"] for s in sports
                    if "cricket" in s.get("key", "").lower() and s.get("active", False)
                ]
                print(f"[CRICKET] Active cricket sports: {active_cricket}")
            else:
                print(f"[CRICKET] Sports list failed: HTTP {r.status_code}")
                active_cricket = CRICKET_SPORT_KEYS[:3]  # fallback to first 3
        except Exception as e:
            print(f"[CRICKET] Sports fetch error: {e}")
            active_cricket = CRICKET_SPORT_KEYS[:3]

        # Fetch odds for each active cricket sport
        for sport_key in active_cricket[:5]:  # limit to 5 to save API quota
            try:
                r = requests.get(
                    f"{ODDS_API_BASE}/sports/{sport_key}/odds",
                    params={
                        "apiKey": self._api_key,
                        "regions": "uk,eu,us,au",
                        "markets": "h2h",
                        "oddsFormat": "decimal",
                    },
                    timeout=10,
                )
                requests_remaining = r.headers.get("x-requests-remaining", requests_remaining)
                requests_used = r.headers.get("x-requests-used", requests_used)

                if r.status_code == 200:
                    events = r.json()
                    parsed = self._parse_events(events, sport_key)
                    all_matches.extend(parsed)
                    print(f"[CRICKET] {sport_key}: {len(parsed)} matches")
                elif r.status_code == 422:
                    print(f"[CRICKET] {sport_key}: no events (off-season)")
                else:
                    print(f"[CRICKET] {sport_key}: HTTP {r.status_code}")
            except Exception as e:
                print(f"[CRICKET] {sport_key} error: {e}")

        print(f"[CRICKET] Total: {len(all_matches)} matches | Quota remaining: {requests_remaining}")
        return {
            "matches": all_matches,
            "requests_remaining": requests_remaining,
            "requests_used": requests_used,
            "error": None,
        }

    def _parse_events(self, events: List[Dict], sport_key: str) -> List[Dict]:
        """Convert raw Odds API events into our flat match format."""
        results = []
        sport_label = sport_key.replace("cricket_", "").replace("_", " ").title()

        for event in events:
            home_team = event.get("home_team", "")
            away_team = event.get("away_team", "")
            commence_time = event.get("commence_time", "")
            bookmakers = event.get("bookmakers", [])

            for bm in bookmakers:
                bm_title = bm.get("title", bm.get("key", "Unknown"))
                for market in bm.get("markets", []):
                    if market.get("key") != "h2h":
                        continue
                    outcomes = {o["name"]: o["price"] for o in market.get("outcomes", [])}
                    odds_home = outcomes.get(home_team)
                    odds_away = outcomes.get(away_team)
                    if odds_home and odds_away:
                        results.append({
                            "source": bm_title,
                            "team_a": home_team,
                            "team_b": away_team,
                            "team_a_odds": round(float(odds_home), 2),
                            "team_b_odds": round(float(odds_away), 2),
                            "match_time": commence_time,
                            "sport": sport_label,
                            "real_odds": True,
                        })

        return results
