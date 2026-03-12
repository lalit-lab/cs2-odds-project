from fastapi import FastAPI, Depends, HTTPException, status, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime, timedelta
import asyncio
import hashlib
import json
import os

from app.database import get_db, init_db, User
from app.scraper import OddsScraper
from app.analysis import OddsAnalyzer
from app.telegram_bot import TelegramNotifier
from app.auth import (
    UserCreate, UserResponse, Token,
    get_password_hash, verify_password,
    create_access_token, ACCESS_TOKEN_EXPIRE_MINUTES
)

app = FastAPI(
    title="CS2 Odds API",
    description="Live betting odds aggregator for CS2 esports",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Components
scraper = OddsScraper()
analyzer = OddsAnalyzer()
telegram_bot = TelegramNotifier()

# In-memory cache
live_odds_cache = []
arbitrage_cache = []
last_update = None
scraping_active = False

# Bookmaker URLs for frontend links
BOOKMAKER_URLS = {
    "GGBet": "https://ggbet.com",
    "Thunderpick": "https://thunderpick.io",
    "1xBet": "https://1xbet.com",
    "Vulkan Bet": "https://vulkanbet.com",
    "Bet20": "https://bet20.com",
    "N1 Bet": "https://n1bet.com",
    "Roobet": "https://roobet.com",
    "Betify": "https://betify.com",
    "Melbet": "https://melbet.com",
    "BC.Game": "https://bc.game",
    "EpicBet": "https://epicbet.com",
    "Vavada": "https://vavada.com",
    "Housebets": "https://housebets.com",
    "ColdBet": "https://coldbet.com",
    "BetLabel": "https://betlabel.com",
    "YBets": "https://ybets.com",
    "2UP": "https://2up.io",
}


# ============================================================================
# WEBSOCKET CONNECTION MANAGER
# ============================================================================

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        # Send current cached data immediately on connect
        if live_odds_cache:
            await websocket.send_json({
                "type": "update",
                "odds": live_odds_cache,
                "arbitrage": arbitrage_cache,
                "timestamp": last_update,
                "connections": len(self.active_connections),
            })

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, data: dict):
        dead = []
        for conn in self.active_connections:
            try:
                await conn.send_json(data)
            except Exception:
                dead.append(conn)
        for conn in dead:
            self.active_connections.remove(conn)


manager = ConnectionManager()


# ============================================================================
# STARTUP
# ============================================================================

@app.on_event("startup")
async def startup_event():
    print("=" * 60)
    print("CS2 Odds API Starting...")
    print("=" * 60)
    init_db()
    print("Server started successfully!")
    print("=" * 60)
    # Auto-start scraper
    asyncio.create_task(auto_start_scraper())


async def auto_start_scraper():
    """Start the scraper automatically after a short delay."""
    global scraping_active
    await asyncio.sleep(2)
    if not scraping_active:
        scraping_active = True
        asyncio.create_task(scraping_loop())
        print("Scraper auto-started.")


# ============================================================================
# ROUTES
# ============================================================================

@app.get("/")
async def serve_dashboard():
    """Serve the live dashboard HTML."""
    return FileResponse("index.html")


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.get("/api/bookmakers")
async def get_bookmakers():
    """Return bookmaker names and their URLs for frontend links."""
    return {"bookmakers": BOOKMAKER_URLS}


# WebSocket endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()  # keep alive / ping-pong
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# ============================================================================
# AUTH ENDPOINTS
# ============================================================================

@app.post("/api/auth/register", response_model=UserResponse)
def register_user(user_data: UserCreate, db: Session = Depends(get_db)):
    existing_user = db.query(User).filter(User.username == user_data.username).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already registered")
    existing_email = db.query(User).filter(User.email == user_data.email).first()
    if existing_email:
        raise HTTPException(status_code=400, detail="Email already registered")
    hashed_password = get_password_hash(user_data.password)
    new_user = User(
        username=user_data.username,
        email=user_data.email,
        password_hash=hashed_password,
        telegram_chat_id=user_data.telegram_chat_id,
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user


@app.post("/api/auth/login", response_model=Token)
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.username == form_data.username).first()
    if not user or not verify_password(form_data.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


# ============================================================================
# ODDS ENDPOINTS
# ============================================================================

@app.get("/api/odds/live")
async def get_live_odds():
    return {
        "message": "Live odds",
        "count": len(live_odds_cache),
        "data": live_odds_cache,
        "timestamp": last_update,
    }


@app.get("/api/arbitrage/current")
async def get_arbitrage_opportunities():
    return {
        "message": "Arbitrage opportunities",
        "count": len(arbitrage_cache),
        "data": arbitrage_cache,
    }


# ============================================================================
# SCRAPER ENDPOINTS
# ============================================================================

@app.post("/api/scraper/start")
async def start_scraper(background_tasks: BackgroundTasks):
    global scraping_active
    if scraping_active:
        return {"message": "Scraper is already running"}
    scraping_active = True
    background_tasks.add_task(scraping_loop)
    return {"message": "Scraper started successfully"}


@app.post("/api/scraper/stop")
async def stop_scraper():
    global scraping_active
    scraping_active = False
    return {"message": "Scraper stopped"}


@app.get("/api/scraper/status")
async def scraper_status():
    return {
        "active": scraping_active,
        "odds_count": len(live_odds_cache),
        "arbitrage_count": len(arbitrage_cache),
        "last_update": last_update,
        "connected_clients": len(manager.active_connections),
    }


# ============================================================================
# BACKGROUND SCRAPING LOOP
# ============================================================================

async def scraping_loop():
    global live_odds_cache, arbitrage_cache, last_update, scraping_active

    # SCRAPE_INTERVAL env var controls update speed:
    # - Local (HLTV): keep 18s (scrape itself takes ~15-20s)
    # - Railway (mock): set SCRAPE_INTERVAL=5 in Railway env vars for near-live feel
    interval = int(os.getenv("SCRAPE_INTERVAL", "18"))
    last_hash = ""
    print(f"Scraping loop started (interval={interval}s)")

    while scraping_active:
        try:
            print("Starting scrape cycle...")
            odds_data = await scraper.scrape_all_sites()

            # Change detection — only broadcast if odds actually changed
            new_hash = hashlib.md5(
                json.dumps(odds_data, sort_keys=True, default=str).encode()
            ).hexdigest()

            if new_hash != last_hash:
                arbitrage_opps = analyzer.detect_arbitrage(odds_data)
                live_odds_cache = odds_data
                arbitrage_cache = arbitrage_opps
                last_update = datetime.utcnow().isoformat()
                last_hash = new_hash

                await manager.broadcast({
                    "type": "update",
                    "odds": odds_data,
                    "arbitrage": arbitrage_opps,
                    "timestamp": last_update,
                    "connections": len(manager.active_connections),
                })
                print(f"Odds changed — {len(odds_data)} entries, {len(arbitrage_opps)} arb opps, pushed to {len(manager.active_connections)} clients")
            else:
                print("No change in odds — skipping broadcast")

            await asyncio.sleep(interval)

        except Exception as e:
            print(f"Scraping error: {e}")
            await asyncio.sleep(interval)

    print("Scraping loop stopped")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
