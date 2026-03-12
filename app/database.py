from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/cs2_odds")

#DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./cs2_odds.db")


engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ============================================================================
# DATABASE MODELS
# ============================================================================

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, index=True, nullable=False)
    email = Column(String(100), unique=True, index=True)
    password_hash = Column(String(255), nullable=False)
    telegram_chat_id = Column(String(50), nullable=True)
    virtual_balance = Column(Float, default=10000.0)
    created_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    
    # Relationships
    alerts = relationship("Alert", back_populates="user", cascade="all, delete-orphan")
    virtual_bets = relationship("VirtualBet", back_populates="user", cascade="all, delete-orphan")


class Match(Base):
    __tablename__ = "matches"
    
    id = Column(Integer, primary_key=True, index=True)
    match_id = Column(String(100), unique=True, index=True, nullable=False)
    team_a = Column(String(100), nullable=False)
    team_b = Column(String(100), nullable=False)
    match_time = Column(DateTime, nullable=False)
    status = Column(String(20), default="upcoming")  # upcoming, live, finished
    winner = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    odds_history = relationship("OddsHistory", back_populates="match", cascade="all, delete-orphan")
    virtual_bets = relationship("VirtualBet", back_populates="match")


class OddsHistory(Base):
    __tablename__ = "odds_history"
    
    id = Column(Integer, primary_key=True, index=True)
    match_id = Column(Integer, ForeignKey("matches.id"), nullable=False)
    source = Column(String(50), nullable=False)  # Betting site name
    team_a_odds = Column(Float, nullable=False)
    team_b_odds = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Relationships
    match = relationship("Match", back_populates="odds_history")


class Alert(Base):
    __tablename__ = "alerts"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    team_name = Column(String(100), nullable=False)
    threshold = Column(Float, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_triggered = Column(DateTime, nullable=True)
    
    # Relationships
    user = relationship("User", back_populates="alerts")


class VirtualBet(Base):
    __tablename__ = "virtual_bets"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    match_id = Column(Integer, ForeignKey("matches.id"), nullable=False)
    selected_team = Column(String(100), nullable=False)
    odds = Column(Float, nullable=False)
    stake = Column(Float, nullable=False)
    potential_win = Column(Float, nullable=False)
    status = Column(String(20), default="pending")  # pending, won, lost
    profit = Column(Float, default=0.0)
    placed_at = Column(DateTime, default=datetime.utcnow)
    settled_at = Column(DateTime, nullable=True)
    
    # Relationships
    user = relationship("User", back_populates="virtual_bets")
    match = relationship("Match", back_populates="virtual_bets")


class ArbitrageOpportunity(Base):
    __tablename__ = "arbitrage_opportunities"
    
    id = Column(Integer, primary_key=True, index=True)
    match_id = Column(Integer, ForeignKey("matches.id"), nullable=False)
    team_a = Column(String(100), nullable=False)
    team_b = Column(String(100), nullable=False)
    best_odds_a = Column(Float, nullable=False)
    best_odds_b = Column(Float, nullable=False)
    source_a = Column(String(50), nullable=False)
    source_b = Column(String(50), nullable=False)
    profit_percent = Column(Float, nullable=False)
    stake_a_percent = Column(Float, nullable=False)
    stake_b_percent = Column(Float, nullable=False)
    detected_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)


# Create all tables
def init_db():
    Base.metadata.create_all(bind=engine)
    print("Database tables created successfully!")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
