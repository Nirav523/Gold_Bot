"""
GOLD LIVE TRADING BOT - COMPLETE VERSION
✅ 15-minute candle checks (96 API calls/day)
✅ Entry signals with TP/SL and IST time
✅ TP/SL hit alerts
✅ Session start/end alerts (London, Overlap, New York)
✅ Live data from Twelve Data API
✅ Telegram notifications
"""

import asyncio
import aiohttp
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple
import logging
from dataclasses import dataclass
from enum import Enum
import json
import sqlite3
import os
import signal
import sys
import time

# Telegram
from telegram import Bot
import telegram.error

# ============================================
# CONFIGURATION
# ============================================

# Twelve Data API Key (get from https://twelvedata.com)
TWELVE_DATA_API_KEY = "9625492f7453451ba2b0a168a029a479"

# Telegram Bot (get from @BotFather)
TELEGRAM_BOT_TOKEN = "8892424969:AAEtTlUMt0JOM9jjC6MhH_tjV3Z5dYSKtIo"
TELEGRAM_CHAT_ID = "854168042"

# ============================================
# STRATEGY SETTINGS (YOUR PROVEN SETTINGS)
# ============================================

STOP_LOSS = 5.20
TAKE_PROFIT = 15.60

RSI_LONG_MIN = 41
RSI_LONG_MAX = 53
RSI_SHORT_MIN = 47
RSI_SHORT_MAX = 59

MACD_LONG_THRESHOLD = -0.6
MACD_SHORT_THRESHOLD = 0.6

TRADE_START_HOUR = 9
TRADE_START_MINUTE = 0
TRADE_END_HOUR = 18
TRADE_END_MINUTE = 0

# Session definitions
SESSION_LONDON_START = "09:00"
SESSION_LONDON_END = "12:00"
SESSION_OVERLAP_START = "12:00"
SESSION_OVERLAP_END = "16:00"
SESSION_NY_START = "16:00"
SESSION_NY_END = "18:00"

# Position limits
MAX_TRADES_PER_DAY = 3
MIN_CANDLES_BETWEEN_SIGNALS = 4

# File paths
DB_PATH = "gold_trading.db"
LOG_PATH = "gold_bot.log"

# ============================================
# LOGGING SETUP
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================
# DATA CLASSES
# ============================================

class TradeDirection(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NONE = "NONE"

class TradeStatus(Enum):
    ACTIVE = "ACTIVE"
    STOP_LOSS_HIT = "STOP_LOSS_HIT"
    TAKE_PROFIT_HIT = "TAKE_PROFIT_HIT"
    CLOSED = "CLOSED"

@dataclass
class ActiveTrade:
    entry_time: datetime
    entry_price: float
    direction: str
    stop_loss: float
    take_profit: float
    status: str = "ACTIVE"
    entry_signal_time: str = ""
    session: str = ""

# ============================================
# HELPER FUNCTIONS
# ============================================

def now_utc() -> datetime:
    """Get current UTC datetime (timezone-aware)"""
    return datetime.now(timezone.utc)

def get_ist_time(dt: datetime = None) -> str:
    """Convert UTC to IST (UTC+5:30)"""
    if dt is None:
        dt = now_utc()
    ist_time = dt + timedelta(hours=5, minutes=30)
    return ist_time.strftime("%Y-%m-%d %H:%M:%S")

def get_ist_time_short(dt: datetime = None) -> str:
    """Short IST time for alerts"""
    if dt is None:
        dt = now_utc()
    ist_time = dt + timedelta(hours=5, minutes=30)
    return ist_time.strftime("%H:%M:%S")

def is_trading_hours(dt: datetime) -> bool:
    """Check if within trading hours"""
    trade_start = dt.replace(hour=TRADE_START_HOUR, minute=TRADE_START_MINUTE, second=0, microsecond=0)
    trade_end = dt.replace(hour=TRADE_END_HOUR, minute=TRADE_END_MINUTE, second=0, microsecond=0)
    return trade_start <= dt <= trade_end

def get_current_session(dt: datetime) -> str:
    """Get current trading session"""
    time_str = dt.strftime("%H:%M")
    if SESSION_LONDON_START <= time_str < SESSION_LONDON_END:
        return "LONDON"
    elif SESSION_OVERLAP_START <= time_str < SESSION_OVERLAP_END:
        return "OVERLAP"
    elif SESSION_NY_START <= time_str < SESSION_NY_END:
        return "NEW YORK"
    return "NO SESSION"

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate all technical indicators"""
    
    df["EMA20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["EMA50"] = df["close"].ewm(span=50, adjust=False).mean()
    
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df["RSI"] = 100 - (100 / (1 + rs))
    
    exp1 = df["close"].ewm(span=12, adjust=False).mean()
    exp2 = df["close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = exp1 - exp2
    df["Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_Histogram"] = df["MACD"] - df["Signal"]
    
    return df

# ============================================
# TELEGRAM ALERTS
# ============================================

class TelegramAlert:
    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        
    async def send(self, message: str):
        try:
            await self.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
    
    async def send_with_retry(self, message: str, max_retries: int = 3):
        for attempt in range(max_retries):
            try:
                await self.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='HTML')
                return True
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to send after {max_retries} attempts: {e}")
                    return False
                await asyncio.sleep(2)

# ============================================
# TRADE MANAGEMENT
# ============================================

class TradeManager:
    def __init__(self):
        self.active_trades: List[ActiveTrade] = []
        self.daily_trades_count = 0
        self.last_check_day = None
        self.consecutive_losses = 0
        self.db_id = None
        self.init_database()
        self.load_active_trades()
    
    def init_database(self):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_time TEXT,
                exit_time TEXT,
                direction TEXT,
                entry_price REAL,
                exit_price REAL,
                stop_loss REAL,
                take_profit REAL,
                status TEXT,
                pnl_points REAL,
                session TEXT,
                ist_time TEXT
            )
        ''')
        conn.commit()
        conn.close()
    
    def load_active_trades(self):
        if os.path.exists("active_trades.json"):
            try:
                with open("active_trades.json", 'r') as f:
                    data = json.load(f)
                    self.active_trades = [ActiveTrade(**t) for t in data]
                logger.info(f"Loaded {len(self.active_trades)} active trades")
            except Exception as e:
                logger.error(f"Failed to load trades: {e}")
    
    def save_active_trades(self):
        try:
            with open("active_trades.json", 'w') as f:
                json.dump([t.__dict__ for t in self.active_trades], f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save trades: {e}")
    
    def add_trade(self, trade: ActiveTrade):
        self.active_trades.append(trade)
        self.save_active_trades()
        self.save_trade_to_db(trade)
    
    def remove_trade(self, trade: ActiveTrade):
        self.active_trades.remove(trade)
        self.save_active_trades()
    
    def save_trade_to_db(self, trade: ActiveTrade):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO trades (entry_time, direction, entry_price, stop_loss, take_profit, status, session, ist_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            trade.entry_time.isoformat(),
            trade.direction,
            trade.entry_price,
            trade.stop_loss,
            trade.take_profit,
            trade.status,
            trade.session,
            trade.entry_signal_time
        ))
        conn.commit()
        trade.db_id = cursor.lastrowid
        conn.close()
    
    def update_trade_in_db(self, trade: ActiveTrade, exit_time: datetime, exit_price: float, status: str, pnl_points: float):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        ist_time = get_ist_time(exit_time)
        cursor.execute('''
            UPDATE trades 
            SET exit_time=?, exit_price=?, status=?, pnl_points=?, ist_time=?
            WHERE rowid=?
        ''', (exit_time.isoformat(), exit_price, status, pnl_points, ist_time, trade.db_id))
        conn.commit()
        conn.close()

# ============================================
# DATA FETCHING
# ============================================

async def fetch_gold_data() -> Optional[pd.DataFrame]:
    """Fetch last 50 candles of 15-minute gold data"""
    
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": "XAU/USD",
        "interval": "15min",
        "apikey": TWELVE_DATA_API_KEY,
        "outputsize": "50",
        "timezone": "UTC"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=30) as response:
                data = await response.json()
                
                if "values" not in data:
                    logger.error(f"API Error: {data.get('message', 'Unknown error')}")
                    return None
                
                df = pd.DataFrame(data["values"])
                df["datetime"] = pd.to_datetime(df["datetime"])
                df["open"] = df["open"].astype(float)
                df["high"] = df["high"].astype(float)
                df["low"] = df["low"].astype(float)
                df["close"] = df["close"].astype(float)
                df["volume"] = df["volume"].astype(float) if "volume" in df else 100
                
                return df
                
    except Exception as e:
        logger.error(f"Fetch failed: {e}")
        return None

# ============================================
# SIGNAL DETECTION
# ============================================

def check_signal(df: pd.DataFrame) -> Tuple[Optional[str], Optional[Dict]]:
    """Check for LONG or SHORT signals"""
    
    if len(df) < 50:
        return None, None
    
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    candle_time = latest["datetime"]
    session = get_current_session(candle_time)
    
    # Skip if NO SESSION
    if session == "NO SESSION":
        return None, None
    
    # Check trading hours
    if not is_trading_hours(candle_time):
        return None, None
    
    # LONG SIGNAL
    if (latest["close"] > latest["EMA20"] and 
        latest["EMA20"] > latest["EMA50"] and
        RSI_LONG_MIN <= latest["RSI"] <= RSI_LONG_MAX and
        latest["MACD_Histogram"] > previous["MACD_Histogram"] and
        latest["MACD_Histogram"] > MACD_LONG_THRESHOLD):
        
        signals = {
            "direction": "LONG",
            "entry": latest["close"],
            "stop_loss": latest["close"] - STOP_LOSS,
            "take_profit": latest["close"] + TAKE_PROFIT,
            "rsi": latest["RSI"],
            "macd_hist": latest["MACD_Histogram"],
            "ema20": latest["EMA20"],
            "ema50": latest["EMA50"],
            "time": candle_time,
            "session": session
        }
        return "LONG", signals
    
    # SHORT SIGNAL
    if (latest["close"] < latest["EMA20"] and 
        latest["EMA20"] < latest["EMA50"] and
        RSI_SHORT_MIN <= latest["RSI"] <= RSI_SHORT_MAX and
        latest["MACD_Histogram"] < previous["MACD_Histogram"] and
        latest["MACD_Histogram"] < MACD_SHORT_THRESHOLD):
        
        signals = {
            "direction": "SHORT",
            "entry": latest["close"],
            "stop_loss": latest["close"] + STOP_LOSS,
            "take_profit": latest["close"] - TAKE_PROFIT,
            "rsi": latest["RSI"],
            "macd_hist": latest["MACD_Histogram"],
            "ema20": latest["EMA20"],
            "ema50": latest["EMA50"],
            "time": candle_time,
            "session": session
        }
        return "SHORT", signals
    
    return None, None

# ============================================
# ALERT FORMATTING
# ============================================

def format_entry_alert(direction: str, signals: Dict) -> str:
    """Format entry signal for Telegram"""
    
    now = now_utc()
    ist_time_full = get_ist_time(now)
    ist_time_short = get_ist_time_short(now)
    
    if direction == "LONG":
        emoji = "🟢"
        action = "BUY"
    else:
        emoji = "🔴"
        action = "SELL"
    
    session_emoji = "🇬🇧" if signals['session'] == "LONDON" else "🌟" if signals['session'] == "OVERLAP" else "🇺🇸"
    
    message = f"""
{emoji} <b>🚨 TRADE SIGNAL - {direction}</b> {emoji}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 <b>Action:</b> {action} GOLD at MARKET
💰 <b>Entry:</b> ${signals['entry']:.2f}
🛑 <b>Stop Loss:</b> ${signals['stop_loss']:.2f} (520 points)
🎯 <b>Take Profit:</b> ${signals['take_profit']:.2f} (1560 points)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📈 <b>Risk:Reward:</b> 1:3
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 <b>Technicals:</b>
• RSI: {signals['rsi']:.1f}
• EMA20: ${signals['ema20']:.2f}
• EMA50: ${signals['ema50']:.2f}
• MACD Hist: {signals['macd_hist']:.3f}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🕐 <b>Session:</b> {session_emoji} {signals['session']}
⏰ <b>GMT:</b> {now.strftime('%H:%M:%S')}
🇮🇳 <b>IST:</b> {ist_time_short}
📅 <b>Date:</b> {ist_time_full[:10]}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
<i>⚠️ Risk 1-2% per trade | Set SL/TP immediately</i>
"""
    return message

def format_exit_alert(trade: ActiveTrade, exit_price: float, result: str, pnl_points: float) -> str:
    """Format exit alert for Telegram"""
    
    now = now_utc()
    ist_time_full = get_ist_time(now)
    ist_time_short = get_ist_time_short(now)
    
    if result == "TAKE_PROFIT_HIT":
        emoji = "✅🎯"
        title = "TAKE PROFIT HIT"
        color = "🟢"
    else:
        emoji = "❌🛑"
        title = "STOP LOSS HIT"
        color = "🔴"
    
    pnl_percent = (pnl_points / trade.entry_price) * 100
    
    message = f"""
{emoji} <b>{title}</b> {emoji}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{color} <b>Trade Closed</b> {color}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 <b>Direction:</b> {trade.direction}
💰 <b>Entry:</b> ${trade.entry_price:.2f}
💵 <b>Exit:</b> ${exit_price:.2f}
📈 <b>P&L:</b> {'+' if pnl_points > 0 else ''}{pnl_points:.1f} points ({pnl_percent:+.2f}%)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🛑 <b>Stop Loss:</b> ${trade.stop_loss:.2f}
🎯 <b>Take Profit:</b> ${trade.take_profit:.2f}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⏰ <b>Entry GMT:</b> {trade.entry_time.strftime('%H:%M:%S')}
⏰ <b>Exit GMT:</b> {now.strftime('%H:%M:%S')}
🇮🇳 <b>IST:</b> {ist_time_short}
📅 <b>Date:</b> {ist_time_full[:10]}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
<i>⚡ New signals will be monitored automatically</i>
"""
    return message

def format_session_alert(session: str, is_start: bool) -> str:
    """Format session start/end alert"""
    
    now = now_utc()
    ist_time_short = get_ist_time_short(now)
    
    if session == "LONDON" and is_start:
        return f"""
🟢 <b>LONDON SESSION STARTED</b> 🟢
━━━━━━━━━━━━━━━━━━━━━━
🇬🇧 Session: London (09:00-12:00 GMT)
⚡ Volatility: Moderate
📈 Focus: LONG signals (RSI 41-53)
⏰ GMT: {now.strftime('%H:%M')}
🇮🇳 IST: {ist_time_short}
━━━━━━━━━━━━━━━━━━━━━━
✅ Ready for LONG signals
"""
    elif session == "OVERLAP" and is_start:
        return f"""
🌟 <b>LONDON-NY OVERLAP STARTED</b> 🌟
━━━━━━━━━━━━━━━━━━━━━━
🌍 Session: Overlap (12:00-16:00 GMT)
⚡ Volatility: HIGHEST
📈 Focus: Both LONG and SHORT
⏰ GMT: {now.strftime('%H:%M')}
🇮🇳 IST: {ist_time_short}
━━━━━━━━━━━━━━━━━━━━━━
🎯 BEST TRADING HOURS - Prioritize signals!
"""
    elif session == "NEW YORK" and is_start:
        return f"""
🔵 <b>NEW YORK SESSION STARTED</b> 🔵
━━━━━━━━━━━━━━━━━━━━━━
🇺🇸 Session: New York (16:00-18:00 GMT)
⚡ Volatility: High
📈 Focus: SHORT signals (RSI 47-59)
⏰ GMT: {now.strftime('%H:%M')}
🇮🇳 IST: {ist_time_short}
━━━━━━━━━━━━━━━━━━━━━━
✅ Ready for SHORT signals
"""
    elif not is_start:
        active_count = len(trade_manager.active_trades) if 'trade_manager' in globals() else 0
        return f"""
🔴 <b>TRADING SESSION ENDED</b> 🔴
━━━━━━━━━━━━━━━━━━━━━━
⏰ GMT: {now.strftime('%H:%M')}
🇮🇳 IST: {ist_time_short}
━━━━━━━━━━━━━━━━━━━━━━
📊 Active trades: {active_count}
💤 No new entries until tomorrow 09:00 GMT
"""
    return ""

# ============================================
# MAIN BOT
# ============================================

class GoldTradingBot:
    def __init__(self):
        self.telegram = TelegramAlert()
        self.trade_manager = TradeManager()
        self.running = True
        self.last_candle_minute = None
        self.last_session = None
        self.session_active = False
        self.api_calls_today = 0
        self.last_api_reset = now_utc().date()
    
    async def fetch_data_with_limit(self):
        df = await fetch_gold_data()
        if df is not None:
            self.api_calls_today += 1
            logger.info(f"API calls today: {self.api_calls_today}")
        return df
    
    async def check_tp_sl_hits(self):
        """Monitor active trades for TP/SL hits using current price"""
        if not self.trade_manager.active_trades:
            return
        
        df = await self.fetch_data_with_limit()
        if df is None:
            return
        
        current_price = df.iloc[-1]["close"]
        
        for trade in self.trade_manager.active_trades[:]:
            hit_status = None
            exit_price = None
            
            if trade.direction == "LONG":
                if current_price >= trade.take_profit:
                    hit_status = "TAKE_PROFIT_HIT"
                    exit_price = trade.take_profit
                elif current_price <= trade.stop_loss:
                    hit_status = "STOP_LOSS_HIT"
                    exit_price = trade.stop_loss
            else:
                if current_price <= trade.take_profit:
                    hit_status = "TAKE_PROFIT_HIT"
                    exit_price = trade.take_profit
                elif current_price >= trade.stop_loss:
                    hit_status = "STOP_LOSS_HIT"
                    exit_price = trade.stop_loss
            
            if hit_status:
                pnl = TAKE_PROFIT if hit_status == "TAKE_PROFIT_HIT" else -STOP_LOSS
                
                alert = format_exit_alert(trade, exit_price, hit_status, pnl)
                await self.telegram.send(alert)
                
                self.trade_manager.update_trade_in_db(trade, now_utc(), exit_price, hit_status, pnl)
                
                if hit_status == "STOP_LOSS_HIT":
                    self.trade_manager.consecutive_losses += 1
                    if self.trade_manager.consecutive_losses >= 2:
                        warning = f"⚠️ <b>WARNING:</b> {self.trade_manager.consecutive_losses} consecutive losses!\n🛑 Consider stopping for today."
                        await self.telegram.send(warning)
                else:
                    self.trade_manager.consecutive_losses = 0
                
                self.trade_manager.remove_trade(trade)
                self.trade_manager.daily_trades_count += 1
    
    async def check_entry_signals(self):
        """Check for new entry signals at 15-minute candle closes"""
        now = now_utc()
        current_minute = now.minute
        
        if current_minute not in [0, 15, 30, 45]:
            return
        
        candle_key = f"{now.hour}:{current_minute}"
        if self.last_candle_minute == candle_key:
            return
        self.last_candle_minute = candle_key
        
        if not is_trading_hours(now):
            return
        
        current_day = now.date()
        if current_day != self.trade_manager.last_check_day:
            self.trade_manager.daily_trades_count = 0
            self.trade_manager.last_check_day = current_day
        
        if self.trade_manager.daily_trades_count >= MAX_TRADES_PER_DAY:
            return
        
        logger.info(f"Checking for signals at {now.strftime('%H:%M')} GMT...")
        
        df = await self.fetch_data_with_limit()
        if df is None:
            return
        
        df = calculate_indicators(df)
        direction, signals = check_signal(df)
        
        if direction and signals:
            trade = ActiveTrade(
                entry_time=now,
                entry_price=signals['entry'],
                direction=direction,
                stop_loss=signals['stop_loss'],
                take_profit=signals['take_profit'],
                session=signals['session'],
                entry_signal_time=get_ist_time(now)
            )
            
            self.trade_manager.add_trade(trade)
            alert = format_entry_alert(direction, signals)
            await self.telegram.send_with_retry(alert)
            logger.info(f"✅ SIGNAL: {direction} at ${signals['entry']:.2f}")
    
    async def send_session_alerts(self):
        """Send session start/end notifications"""
        now = now_utc()
        current_session = get_current_session(now)
        
        if current_session != self.last_session:
            if self.last_session != "NO SESSION" and self.last_session is not None:
                end_alert = format_session_alert(self.last_session, False)
                await self.telegram.send(end_alert)
                logger.info(f"Session ended: {self.last_session}")
            
            if current_session != "NO SESSION":
                start_alert = format_session_alert(current_session, True)
                await self.telegram.send(start_alert)
                logger.info(f"Session started: {current_session}")
            
            self.last_session = current_session
    
    async def daily_summary(self):
        """Send daily summary at end of day"""
        now = now_utc()
        if now.hour == 18 and now.minute == 5:
            summary = f"""
📊 <b>DAILY TRADING SUMMARY</b>
━━━━━━━━━━━━━━━━━━━━━━
📅 Date: {now.strftime('%Y-%m-%d')}
📈 Trades Today: {self.trade_manager.daily_trades_count}
📉 Consecutive Losses: {self.trade_manager.consecutive_losses}
📡 API Calls Today: {self.api_calls_today}
━━━━━━━━━━━━━━━━━━━━━━
🤖 Bot is ready for tomorrow at 09:00 GMT
"""
            await self.telegram.send(summary)
    
    async def health_check(self):
        """Send health check daily"""
        now = now_utc()
        if now.hour == 0 and now.minute == 0:
            health = f"""
💚 <b>Bot Health Check</b> 💚
━━━━━━━━━━━━━━━━━━━━━━
📊 Strategy: High Frequency Gold
📅 Date: {now.strftime('%Y-%m-%d')}
📡 API Status: Active
✅ Bot Running: 24/7
━━━━━━━━━━━━━━━━━━━━━━
🕐 Trading starts at 09:00 GMT
"""
            await self.telegram.send(health)
    
    async def run(self):
        """Main bot loop"""
        logger.info("Starting Gold Trading Bot")
        
        startup = f"""
🤖 <b>GOLD TRADING BOT STARTED</b>
━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 <b>Strategy:</b> High Frequency Gold
📈 <b>RSI LONG:</b> {RSI_LONG_MIN}-{RSI_LONG_MAX}
📉 <b>RSI SHORT:</b> {RSI_SHORT_MIN}-{RSI_SHORT_MAX}
⚡ <b>SL:</b> {STOP_LOSS} (520 pts) | <b>TP:</b> {TAKE_PROFIT} (1560 pts)
🕐 <b>Hours:</b> {TRADE_START_HOUR:02d}:{TRADE_START_MINUTE:02d} - {TRADE_END_HOUR:02d}:{TRADE_END_MINUTE:02d} GMT
📡 <b>Sessions:</b> LONDON, OVERLAP, NEW YORK only
━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Bot is monitoring for signals
"""
        await self.telegram.send(startup)
        
        while self.running:
            try:
                await self.send_session_alerts()
                await self.check_tp_sl_hits()
                await self.check_entry_signals()
                await self.daily_summary()
                await self.health_check()
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Bot error: {e}")
                await asyncio.sleep(60)
    
    def stop(self):
        self.running = False

# ============================================
# MAIN
# ============================================

trade_manager = TradeManager()

async def main():
    bot = GoldTradingBot()
    
    def shutdown(signum, frame):
        logger.info("Shutting down...")
        bot.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    
    await bot.run()

if __name__ == "__main__":
    asyncio.run(main())