from telegram import Bot
from telegram.error import TelegramError
import os

class TelegramNotifier:
    def __init__(self, bot_token: str = None):
        if bot_token is None:
            bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        
        if bot_token:
            self.bot = Bot(token=bot_token)
        else:
            self.bot = None
            print("⚠️ Telegram bot token not configured")
    
    async def send_arbitrage_alert(self, chat_id: str, arbitrage: dict):
        """Send arbitrage opportunity notification"""
        if not self.bot:
            print("⚠️ Cannot send Telegram message - bot not configured")
            return False
            
        message = f"""
🚨 *ARBITRAGE OPPORTUNITY DETECTED* 🚨

*Match:* {arbitrage['team_a']} vs {arbitrage['team_b']}

*Guaranteed Profit:* {arbitrage['profit_percent']}%

*Bet 1:*
- Team: {arbitrage['team_a']}
- Odds: {arbitrage['best_odds_a']}
- Platform: {arbitrage['source_a']}
- Stake: {arbitrage['stake_a_percent']}%

*Bet 2:*
- Team: {arbitrage['team_b']}
- Odds: {arbitrage['best_odds_b']}
- Platform: {arbitrage['source_b']}
- Stake: {arbitrage['stake_b_percent']}%

⚠️ Act fast - odds may change!
        """
        
        try:
            await self.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode='Markdown'
            )
            return True
        except TelegramError as e:
            print(f"Failed to send Telegram message: {e}")
            return False
    
    async def send_custom_alert(self, chat_id: str, alert: dict):
        """Send custom user alert"""
        if not self.bot:
            print("⚠️ Cannot send Telegram message - bot not configured")
            return False
            
        message = f"""
🔔 *ODDS ALERT TRIGGERED* 🔔

Your alert for *{alert['team_name']}* has been triggered!

*Current Odds:* {alert['current_odds']}
*Your Threshold:* {alert['threshold']}
*Platform:* {alert['source']}

Check the app for more details!
        """
        
        try:
            await self.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode='Markdown'
            )
            return True
        except TelegramError as e:
            print(f"Failed to send Telegram message: {e}")
            return False