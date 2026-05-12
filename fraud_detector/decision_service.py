"""
🎯 DECISION SERVICE
Kết hợp Rule Engine + ML Model + Gemini LLM để ra quyết định cuối cùng
"""

import json
import logging
from typing import Dict, Any, Tuple
from datetime import datetime

try:
    import google.generativeai as genai
except ImportError:
    genai = None

logger = logging.getLogger(__name__)


class DecisionService:
    """
    Combined decision maker:
    1. Rule Engine (R1-R7) → rule_score (0-100)
    2. ML Model (XGBoost) → fraud_probability (0-1)
    3. Merge scores → combined_score
    4. Generate alert message via Gemini LLM
    """
    
    def __init__(self, 
                 rule_engine,
                 ml_model_loader,
                 gemini_api_key: str = None):
        """
        Initialize decision service.
        
        Args:
            rule_engine: RuleEngine instance
            ml_model_loader: MLModelLoader instance
            gemini_api_key: Gemini API key for LLM alerts
        """
        self.rule_engine = rule_engine
        self.ml_model = ml_model_loader
        self.gemini_api_key = gemini_api_key
        
        if gemini_api_key and genai:
            genai.configure(api_key=gemini_api_key)
            logger.info("✅ Gemini LLM initialized")
        elif not genai:
            logger.warning("⚠️  Gemini library not installed - LLM alerts disabled")
    
    def decide(self, 
               enriched_event: Dict[str, Any],
               ml_features: Dict[str, float] = None) -> Dict[str, Any]:
        """
        Make final fraud detection decision.
        
        Args:
            enriched_event: Transaction event enriched with user/merchant info
            ml_features: Feature vector for ML model
        
        Returns:
            {
                "transaction_id": str,
                "cc_num": str,
                "timestamp": str,
                
                "rule_evaluation": {...},          # From RuleEngine
                "ml_evaluation": {...},            # From ML Model
                
                "combined_score": 0-100,           # Weighted merge of rule + ML
                "weight_rules": 0.6,
                "weight_ml": 0.4,
                
                "final_decision": "BLOCK" | "REVIEW" | "ALLOW",
                "confidence": 0.0-1.0,
                
                "alert_message": "...",            # Gemini generated (if fraud)
                "rule_triggers": [...],
                
                "timestamp_decision": str
            }
        """
        try:
            # Get current timestamp
            decision_timestamp = datetime.utcnow().isoformat()
            
            # ==================== RULE EVALUATION ====================
            rule_result = self.rule_engine.evaluate(enriched_event)
            rule_score = rule_result['total_rule_score']  # 0-100
            
            # ==================== ML EVALUATION ====================
            ml_score = 0.0
            ml_result = {}
            
            if ml_features:
                try:
                    ml_score = self.ml_model.predict(ml_features)  # 0-1
                    ml_result = {
                        'fraud_probability': round(ml_score, 4),
                        'model_version': self.ml_model.model_version,
                        'confidence': self._calculate_ml_confidence(ml_score)
                    }
                except Exception as e:
                    logger.warning(f"⚠️  ML prediction failed: {str(e)}")
                    ml_result = {'fraud_probability': 0.0, 'error': str(e)}
            
            # ==================== COMBINE SCORES ====================
            weight_rules = 0.6
            weight_ml = 0.4
            
            # Normalize ML score (0-1) to 0-100
            ml_score_normalized = ml_score * 100
            
            # Weighted combination
            combined_score = (weight_rules * rule_score) + (weight_ml * ml_score_normalized)
            combined_score = min(combined_score, 100)  # Cap at 100
            
            # ==================== DETERMINE DECISION ====================
            final_decision, confidence = self._determine_decision(
                combined_score,
                rule_result['triggered_rules'],
                ml_score
            )
            
            # ==================== GENERATE ALERT ====================
            alert_message = ""
            if final_decision in ['BLOCK', 'REVIEW']:
                alert_message = self._generate_alert_message(
                    enriched_event,
                    rule_result,
                    ml_result,
                    combined_score,
                    final_decision
                )
            
            # ==================== BUILD RESPONSE ====================
            decision_output = {
                'transaction_id': enriched_event.get('transaction_id', 'unknown'),
                'cc_num': enriched_event.get('cc_num', ''),
                'timestamp_transaction': enriched_event.get('trans_date_trans_time', ''),
                
                'rule_evaluation': {
                    'total_rule_score': rule_score,
                    'triggered_rules': rule_result['triggered_rules'],
                    'rule_details': rule_result['rule_details']
                },
                
                'ml_evaluation': ml_result,
                
                'combined_score': round(combined_score, 2),
                'score_components': {
                    'rule_score': round(rule_score, 2),
                    'ml_score_normalized': round(ml_score_normalized, 2),
                    'weight_rules': weight_rules,
                    'weight_ml': weight_ml
                },
                
                'final_decision': final_decision,
                'confidence': round(confidence, 2),
                
                'alert_message': alert_message,
                'should_send_alert': bool(alert_message),
                
                'timestamp_decision': decision_timestamp
            }
            
            logger.info(
                f"✅ Decision made: {final_decision} (score: {combined_score:.0f}) "
                f"for {enriched_event.get('cc_num', 'unknown')}"
            )
            
            return decision_output
        
        except Exception as e:
            logger.error(f"❌ Error in decision making: {str(e)}")
            return {
                'error': str(e),
                'final_decision': 'REVIEW',  # Default to REVIEW if error
                'timestamp_decision': datetime.utcnow().isoformat()
            }
    
    # ==================== DECISION LOGIC ====================
    
    def _determine_decision(self, 
                           combined_score: float,
                           triggered_rules: list,
                           ml_score: float) -> Tuple[str, float]:
        """
        Determine final decision based on combined score and confidence.
        
        Returns: (decision, confidence)
        """
        # Higher score = higher fraud risk
        if combined_score >= 80:
            decision = 'BLOCK'
            confidence = min(combined_score / 100, 1.0)
        elif combined_score >= 50:
            decision = 'REVIEW'
            confidence = combined_score / 100
        else:
            decision = 'ALLOW'
            # For ALLOW, confidence is inverse
            confidence = 1.0 - (combined_score / 100)
        
        return decision, confidence
    
    # ==================== ALERT GENERATION ====================
    
    def _generate_alert_message(self,
                               event: Dict[str, Any],
                               rule_result: Dict[str, Any],
                               ml_result: Dict[str, Any],
                               combined_score: float,
                               decision: str) -> str:
        """
        Generate human-readable alert message using Gemini LLM.
        """
        try:
            if not genai or not self.gemini_api_key:
                return self._generate_alert_message_template(
                    event, rule_result, ml_result, combined_score, decision
                )
            
            # Prepare context for LLM
            context = {
                'transaction': {
                    'amount': event.get('amt', 0),
                    'merchant': event.get('merchant', 'Unknown'),
                    'time': event.get('trans_date_trans_time', ''),
                    'location': f"({event.get('lat')}, {event.get('long')})"
                },
                'triggered_rules': rule_result.get('triggered_rules', []),
                'ml_fraud_probability': ml_result.get('fraud_probability', 0),
                'combined_risk_score': combined_score,
                'decision': decision
            }
            
            prompt = f"""
            You are a fraud detection expert. Generate a concise alert message for a suspicious transaction.
            
            Context: {json.dumps(context)}
            
            Rules triggered: {', '.join(rule_result.get('triggered_rules', []))}
            ML Fraud Probability: {ml_result.get('fraud_probability', 0):.1%}
            Combined Risk Score: {combined_score:.0f}/100
            Decision: {decision}
            
            Generate a brief (2-3 sentences) alert message explaining why this transaction is flagged.
            Be specific about the risk factors.
            """
            
            model = genai.GenerativeModel('gemini-pro')
            response = model.generate_content(prompt)
            
            alert = response.text.strip()
            logger.debug(f"✅ LLM alert generated")
            return alert
        
        except Exception as e:
            logger.warning(f"⚠️  LLM alert generation failed: {str(e)}")
            # Fall back to template
            return self._generate_alert_message_template(
                event, rule_result, ml_result, combined_score, decision
            )
    
    @staticmethod
    def _generate_alert_message_template(event: Dict[str, Any],
                                        rule_result: Dict[str, Any],
                                        ml_result: Dict[str, Any],
                                        combined_score: float,
                                        decision: str) -> str:
        """
        Generate alert message using template (fallback from LLM).
        """
        amt = event.get('amt', 0)
        merchant = event.get('merchant', 'Unknown')
        cc_masked = event.get('cc_num', 'xxxx')
        if len(cc_masked) >= 8:
            cc_masked = f"{cc_masked[:4]}****{cc_masked[-4:]}"
        
        triggered = rule_result.get('triggered_rules', [])
        ml_fraud_prob = ml_result.get('fraud_probability', 0)
        
        triggered_str = ', '.join(triggered[:3]) if triggered else 'Multiple factors'
        
        message = (
            f"🚨 FRAUD ALERT - Card {cc_masked}\n"
            f"Amount: ₫{amt:,.0f} at {merchant}\n"
            f"Risk: {triggered_str} "
            f"(ML: {ml_fraud_prob:.1%}, Score: {combined_score:.0f}/100)\n"
            f"Action: {decision}"
        )
        
        return message
    
    @staticmethod
    def _calculate_ml_confidence(fraud_probability: float) -> str:
        """Calculate confidence level from ML probability."""
        if fraud_probability >= 0.8:
            return 'HIGH'
        elif fraud_probability >= 0.5:
            return 'MEDIUM'
        elif fraud_probability >= 0.3:
            return 'LOW'
        else:
            return 'VERY_LOW'


class MLModelLoader:
    """Wrapper around ML model for inference."""
    
    def __init__(self, model_path: str = None, model_version: str = '0.96'):
        """
        Load ML model.
        
        Args:
            model_path: Path to model file
            model_version: Model version identifier
        """
        self.model_path = model_path or 'fraud_detector/best_fraud_model_096.json'
        self.model_version = model_version
        
        # TODO: Load actual model
        self.model = None
        
        logger.info(f"✅ ML Model loaded: v{model_version}")
    
    def predict(self, features: Dict[str, float]) -> float:
        """
        Predict fraud probability.
        
        Args:
            features: Feature vector
        
        Returns:
            Fraud probability (0-1)
        """
        # TODO: Implement actual prediction
        # For now, return placeholder
        return 0.5


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    
    # Mock components for testing
    from rule_engine import RuleEngine
    
    rule_engine = RuleEngine()
    ml_loader = MLModelLoader()
    
    decision_service = DecisionService(rule_engine, ml_loader)
    
    # Test
    test_event = {
        'transaction_id': 'txn_001',
        'cc_num': '4532015112830366',
        'amt': 150_000_000,
        'merchant': 'Casino Royale',
        'trans_date_trans_time': '2024-05-12T22:30:00',
        'lat': 10.7128,
        'long': 106.7060,
        'user_history_24h': {'total_amt': 400_000_000, 'count': 5},
        'user_history_7d': {'avg_amt': 100_000_000, 'std_dev_amt': 50_000_000, 'count': 10},
        'recent_locations': []
    }
    
    test_features = {
        'feature1': 0.5,
        'feature2': 0.3
    }
    
    result = decision_service.decide(test_event, test_features)
    print(f"\nDecision result:\n{json.dumps(result, indent=2)}")
