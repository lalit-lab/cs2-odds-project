from typing import List, Dict
import numpy as np
from sklearn.linear_model import LinearRegression

class OddsAnalyzer:
    
    @staticmethod
    def detect_arbitrage(odds_data: List[Dict]) -> List[Dict]:
        """
        Detect arbitrage opportunities across different sites
        """
        # Group by match
        matches = {}
        for odds in odds_data:
            match_key = f"{odds['team_a']}_vs_{odds['team_b']}"
            if match_key not in matches:
                matches[match_key] = {
                    "team_a": odds["team_a"],
                    "team_b": odds["team_b"],
                    "odds": []
                }
            matches[match_key]["odds"].append(odds)
        
        arbitrage_opportunities = []
        
        for match_key, match_data in matches.items():
            # Find best odds for each team
            best_a = max(match_data["odds"], key=lambda x: x["team_a_odds"])
            best_b = max(match_data["odds"], key=lambda x: x["team_b_odds"])
            
            # Calculate implied probabilities
            implied_prob_a = 1 / best_a["team_a_odds"]
            implied_prob_b = 1 / best_b["team_b_odds"]
            total_prob = implied_prob_a + implied_prob_b
            
            # Check for arbitrage
            if total_prob < 1:
                profit_percent = ((1 / total_prob) - 1) * 100
                stake_a_percent = (implied_prob_a / total_prob) * 100
                stake_b_percent = (implied_prob_b / total_prob) * 100
                
                arbitrage_opportunities.append({
                    "team_a": match_data["team_a"],
                    "team_b": match_data["team_b"],
                    "best_odds_a": best_a["team_a_odds"],
                    "best_odds_b": best_b["team_b_odds"],
                    "source_a": best_a["source"],
                    "source_b": best_b["source"],
                    "profit_percent": round(profit_percent, 2),
                    "stake_a_percent": round(stake_a_percent, 2),
                    "stake_b_percent": round(stake_b_percent, 2)
                })
        
        return arbitrage_opportunities
    
    @staticmethod
    def calculate_trend(historical_odds: List[tuple]) -> Dict:
        """
        Calculate odds trend using linear regression
        """
        if len(historical_odds) < 3:
            return {"trend": "insufficient_data", "slope": 0}
        
        times = np.array([(t[0] - historical_odds[0][0]).total_seconds() 
                         for t in historical_odds]).reshape(-1, 1)
        odds_values = np.array([t[1] for t in historical_odds])
        
        model = LinearRegression()
        model.fit(times, odds_values)
        slope = model.coef_[0]
        
        if slope > 0.01:
            trend = "drifting"
        elif slope < -0.01:
            trend = "shortening"
        else:
            trend = "stable"
        
        return {
            "trend": trend,
            "slope": round(slope, 4),
            "prediction": round(model.predict([[times[-1][0] + 3600]])[0], 2)
        }