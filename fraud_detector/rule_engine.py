"""
⚙️ RULE-BASED ENGINE
Kiểm tra các quy tắc business logic (Amount, Velocity, Geographic, etc)
"""

import logging
from typing import Dict, List, Any, Tuple
from datetime import datetime
import math

logger = logging.getLogger(__name__)


class RuleEngine:
    """Rule-based fraud detection engine."""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Khởi tạo rule engine với configurable thresholds.
        
        Args:
            config: Dictionary with rule thresholds
        """
        # Default thresholds (có thể override via ConfigMap)
        self.config = config or {
            'R1_AMOUNT_THRESHOLD': 100_000_000,      # 100M VND
            'R1_AMOUNT_WEIGHT': 100,
            
            'R2_VELOCITY_AMOUNT_24H': 500_000_000,   # 500M in 24h
            'R2_VELOCITY_AMOUNT_WEIGHT': 50,
            
            'R3_VELOCITY_COUNT_1MIN': 10,             # 10 transactions in 1 min
            'R3_VELOCITY_COUNT_WEIGHT': 75,
            
            'R4_GEOGRAPHIC_DISTANCE_KM': 500,         # km
            'R4_GEOGRAPHIC_TIME_WINDOW': 5,           # minutes
            'R4_GEOGRAPHIC_WEIGHT': 75,
            
            'R5_TIME_CHECK_START': 6,                 # 6 AM
            'R5_TIME_CHECK_END': 23,                  # 11 PM
            'R5_TIME_CHECK_WEIGHT': 30,
            
            'R6_MERCHANT_RISK_NEW_USER_WEIGHT': 40,
            'R6_MERCHANT_RISK_HIGH_RISK_WEIGHT': 60,
            
            'R7_PATTERN_BREAK_SIGMA': 3,              # 3 standard deviations
            'R7_PATTERN_BREAK_WEIGHT': 50,
        }
    
    def evaluate(self, enriched_event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate all rules và return combined result.
        
        Args:
            enriched_event: Transaction event enriched with user/merchant history from Redis
        
        Returns:
            {
                "total_rule_score": 0-100,
                "triggered_rules": ["R1_AMOUNT_THRESHOLD", ...],
                "rule_details": {...},
                "rule_decision": "BLOCK" | "REVIEW" | "PASS"
            }
        """
        total_score = 0
        triggered_rules = []
        rule_details = {}
        
        # R1: Amount Threshold
        r1_result = self._rule_amount_threshold(enriched_event)
        if r1_result['triggered']:
            total_score += r1_result['score']
            triggered_rules.append('R1_AMOUNT_THRESHOLD')
        rule_details['R1'] = r1_result
        
        # R2: 24h Velocity (Amount)
        r2_result = self._rule_velocity_amount_24h(enriched_event)
        if r2_result['triggered']:
            total_score += r2_result['score']
            triggered_rules.append('R2_VELOCITY_AMOUNT_24H')
        rule_details['R2'] = r2_result
        
        # R3: 1min Velocity (Count)
        r3_result = self._rule_velocity_count_1min(enriched_event)
        if r3_result['triggered']:
            total_score += r3_result['score']
            triggered_rules.append('R3_VELOCITY_COUNT_1MIN')
        rule_details['R3'] = r3_result
        
        # R4: Geographic Check
        r4_result = self._rule_geographic(enriched_event)
        if r4_result['triggered']:
            total_score += r4_result['score']
            triggered_rules.append('R4_GEOGRAPHIC')
        rule_details['R4'] = r4_result
        
        # R5: Time-based Check
        r5_result = self._rule_time_check(enriched_event)
        if r5_result['triggered']:
            total_score += r5_result['score']
            triggered_rules.append('R5_TIME_CHECK')
        rule_details['R5'] = r5_result
        
        # R6: Merchant Risk + New User
        r6_result = self._rule_merchant_risk(enriched_event)
        if r6_result['triggered']:
            total_score += r6_result['score']
            triggered_rules.append('R6_MERCHANT_RISK')
        rule_details['R6'] = r6_result
        
        # R7: Pattern Break (deviation from user average)
        r7_result = self._rule_pattern_break(enriched_event)
        if r7_result['triggered']:
            total_score += r7_result['score']
            triggered_rules.append('R7_PATTERN_BREAK')
        rule_details['R7'] = r7_result
        
        # Cap score at 100
        total_score = min(total_score, 100)
        
        # Determine decision
        if total_score >= 100:
            rule_decision = 'BLOCK'
        elif total_score >= 50:
            rule_decision = 'REVIEW'
        else:
            rule_decision = 'PASS'
        
        return {
            'total_rule_score': total_score,
            'triggered_rules': triggered_rules,
            'rule_details': rule_details,
            'rule_decision': rule_decision
        }
    
    # ==================== RULES ====================
    
    def _rule_amount_threshold(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        R1: Amount Threshold
        Nếu 1 giao dịch > 100M thì có vấn đề (immediate block candidate)
        """
        amt = event.get('amt', 0)
        threshold = self.config['R1_AMOUNT_THRESHOLD']
        
        if amt > threshold:
            return {
                'triggered': True,
                'score': self.config['R1_AMOUNT_WEIGHT'],
                'reason': f'Transaction amount {amt:,.0f} exceeds threshold {threshold:,.0f}',
                'value': amt,
                'threshold': threshold
            }
        
        return {
            'triggered': False,
            'score': 0,
            'reason': 'Amount within threshold'
        }
    
    def _rule_velocity_amount_24h(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        R2: 24h Amount Velocity
        Nếu tổng giao dịch trong 24h > 500M thì nghi ngờ
        """
        history_24h = event.get('user_history_24h') or {}
        total_amt_24h = history_24h.get('total_amt', 0) + event.get('amt', 0)
        threshold = self.config['R2_VELOCITY_AMOUNT_24H']
        
        if total_amt_24h > threshold:
            return {
                'triggered': True,
                'score': self.config['R2_VELOCITY_AMOUNT_WEIGHT'],
                'reason': f'24h amount velocity {total_amt_24h:,.0f} exceeds {threshold:,.0f}',
                'total_24h': total_amt_24h,
                'threshold': threshold,
                'count_24h': history_24h.get('count', 0)
            }
        
        return {
            'triggered': False,
            'score': 0,
            'reason': '24h amount velocity normal'
        }
    
    def _rule_velocity_count_1min(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        R3: 1-minute Transaction Count Velocity
        Nếu có >10 giao dịch trong 1 phút (suspicious pattern)
        """
        # Note: Trong thực tế cần windowing function từ Flink
        # Ở đây chỉ placeholder - sẽ implement trong Flink window
        return {
            'triggered': False,
            'score': 0,
            'reason': 'Implemented in Flink windowing',
            'note': 'Requires tumbling window in stream processing'
        }
    
    def _rule_geographic(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        R4: Geographic Impossibility Check
        Nếu 2 giao dịch cách xa >500km trong vòng 5 phút thì impossible
        
        Formula: distance = sqrt((lat2-lat1)^2 + (long2-long1)^2) * 111 km/degree
        """
        locations = event.get('recent_locations', [])
        
        if not locations or len(locations) < 1:
            return {
                'triggered': False,
                'score': 0,
                'reason': 'No location history available'
            }
        
        try:
            current_lat = event.get('lat')
            current_long = event.get('long')
            current_time = datetime.fromisoformat(event.get('trans_date_trans_time', '2099-01-01T00:00:00'))
            
            last_location = locations[0]
            last_lat = last_location.get('lat')
            last_long = last_location.get('long')
            last_time = datetime.fromisoformat(last_location.get('timestamp', '2099-01-01T00:00:00'))
            
            # Calculate distance
            distance_km = self._haversine_distance(current_lat, current_long, last_lat, last_long)
            
            # Calculate time difference
            time_diff_minutes = (current_time - last_time).total_seconds() / 60
            
            # Check if impossible movement
            max_distance_km = self.config['R4_GEOGRAPHIC_DISTANCE_KM']
            max_time_window_minutes = self.config['R4_GEOGRAPHIC_TIME_WINDOW']
            
            # Speed check: km per minute
            if time_diff_minutes > 0:
                speed_kmh = (distance_km / time_diff_minutes) * 60
                # Max commercial flight speed ~900 km/h, assume max realistic 1000 km/h
                if speed_kmh > 1000:
                    return {
                        'triggered': True,
                        'score': self.config['R4_GEOGRAPHIC_WEIGHT'],
                        'reason': f'Impossible speed {speed_kmh:.0f} km/h (distance {distance_km:.0f}km in {time_diff_minutes:.1f}min)',
                        'distance_km': round(distance_km, 2),
                        'time_diff_minutes': round(time_diff_minutes, 2),
                        'speed_kmh': round(speed_kmh, 0)
                    }
            
            return {
                'triggered': False,
                'score': 0,
                'reason': f'Geographic check OK (distance {distance_km:.0f}km)',
                'distance_km': round(distance_km, 2),
                'time_diff_minutes': round(time_diff_minutes, 2)
            }
        
        except Exception as e:
            logger.error(f"❌ Error in geographic rule: {str(e)}")
            return {
                'triggered': False,
                'score': 0,
                'reason': f'Geographic check error: {str(e)}'
            }
    
    def _rule_time_check(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        R5: Time-based Check
        Nếu giao dịch ngoài giờ bình thường (6AM-11PM) thì nghi ngờ
        """
        trans_time_str = event.get('trans_date_trans_time')
        
        try:
            trans_time = datetime.fromisoformat(trans_time_str)
            hour = trans_time.hour
            
            start_hour = self.config['R5_TIME_CHECK_START']
            end_hour = self.config['R5_TIME_CHECK_END']
            
            if not (start_hour <= hour < end_hour):
                return {
                    'triggered': True,
                    'score': self.config['R5_TIME_CHECK_WEIGHT'],
                    'reason': f'Transaction at unusual hour: {hour}:00 (normal: {start_hour}:00-{end_hour}:00)',
                    'hour': hour,
                    'normal_hours': f'{start_hour}:00-{end_hour}:00'
                }
            
            return {
                'triggered': False,
                'score': 0,
                'reason': f'Transaction at normal hour: {hour}:00'
            }
        
        except Exception as e:
            logger.error(f"❌ Error in time check rule: {str(e)}")
            return {
                'triggered': False,
                'score': 0,
                'reason': f'Time check error: {str(e)}'
            }
    
    def _rule_merchant_risk(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        R6: Merchant Risk + New User
        - High-risk merchant (e.g., bars, casinos) + new user = suspicious
        - New user (trans_count_7d == 1) buying at high-risk merchant
        """
        merchant_info = event.get('merchant_info') or {}
        history_7d = event.get('user_history_7d') or {}
        
        is_high_risk = merchant_info.get('high_risk_flag', False)
        trans_count_7d = history_7d.get('count', 0)
        is_new_user = trans_count_7d <= 1
        
        if is_high_risk and is_new_user:
            return {
                'triggered': True,
                'score': self.config['R6_MERCHANT_RISK_WEIGHT'],
                'reason': 'High-risk merchant + new user',
                'merchant': merchant_info.get('merchant'),
                'merchant_category': merchant_info.get('category'),
                'user_transactions_7d': trans_count_7d
            }
        
        return {
            'triggered': False,
            'score': 0,
            'reason': 'Merchant risk check OK'
        }
    
    def _rule_pattern_break(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        R7: Pattern Break Detection
        Nếu giao dịch lệch hơn 3σ so với user average → suspicious
        
        Formula: z_score = (x - mean) / std_dev
        """
        history_7d = event.get('user_history_7d') or {}
        current_amt = event.get('amt', 0)
        
        avg_amt = history_7d.get('avg_amt', 0)
        std_dev_amt = history_7d.get('std_dev_amt', 0)
        
        if avg_amt == 0 or std_dev_amt == 0:
            return {
                'triggered': False,
                'score': 0,
                'reason': 'Insufficient history for pattern analysis'
            }
        
        sigma_threshold = self.config['R7_PATTERN_BREAK_SIGMA']
        z_score = (current_amt - avg_amt) / std_dev_amt
        
        if abs(z_score) > sigma_threshold:
            return {
                'triggered': True,
                'score': self.config['R7_PATTERN_BREAK_WEIGHT'],
                'reason': f'Transaction amount breaks user pattern (z-score: {z_score:.2f}σ)',
                'current_amt': current_amt,
                'avg_amt': round(avg_amt, 0),
                'std_dev': round(std_dev_amt, 0),
                'z_score': round(z_score, 2)
            }
        
        return {
            'triggered': False,
            'score': 0,
            'reason': f'Amount within user pattern (z-score: {z_score:.2f}σ)'
        }
    
    # ==================== UTILITIES ====================
    
    @staticmethod
    def _haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate distance between 2 lat/long points in kilometers.
        Haversine formula.
        """
        if not all([lat1, lon1, lat2, lon2]):
            return 0
        
        R = 6371  # Earth radius in km
        
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)
        
        a = (math.sin(delta_lat / 2) ** 2 +
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
        c = 2 * math.asin(math.sqrt(a))
        
        return R * c


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    
    engine = RuleEngine()
    
    # Test event
    test_event = {
        'amt': 150_000_000,  # 150M - exceeds threshold
        'trans_date_trans_time': '2024-05-12T22:30:00',  # Outside business hours
        'lat': 10.7128,
        'long': 106.7060,
        'user_history_24h': {'total_amt': 400_000_000, 'count': 5},
        'user_history_7d': {'avg_amt': 100_000_000, 'std_dev_amt': 50_000_000, 'count': 10},
        'recent_locations': [
            {
                'lat': 10.7128,
                'long': 106.7060,
                'timestamp': '2024-05-12T20:30:00'
            }
        ],
        'merchant_info': {'high_risk_flag': True, 'merchant': 'Casino Royale'},
    }
    
    result = engine.evaluate(test_event)
    print(f"Rule evaluation result:\n{result}")
