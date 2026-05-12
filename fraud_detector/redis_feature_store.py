"""
🔴 REDIS FEATURE STORE
Caching user history và merchant info để Flink enrichment tham khảo.
"""

import json
import redis
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class RedisFeatureStore:
    """Redis feature store cho fraud detection."""
    
    def __init__(self, host='redis.storage.svc.cluster.local', port=6379, db=0, password=None):
        """
        Khởi tạo Redis connection.
        
        Args:
            host: Redis host
            port: Redis port
            db: Database number
            password: Password (if auth required)
        """
        try:
            self.redis_client = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True,
                health_check_interval=30
            )
            # Test connection
            self.redis_client.ping()
            logger.info(f"✅ Redis connected: {host}:{port}")
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {str(e)}")
            raise
    
    # ==================== USER PROFILE ====================
    
    def get_user_profile(self, cc_num: str) -> Optional[Dict[str, Any]]:
        """
        Lấy hồ sơ người dùng từ Redis.
        
        Key: user:{cc_num}:profile
        TTL: 86400s (24 hours)
        """
        try:
            data = self.redis_client.get(f"user:{cc_num}:profile")
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"❌ Error getting user profile: {str(e)}")
            return None
    
    def set_user_profile(self, cc_num: str, profile: Dict[str, Any], ttl: int = 86400):
        """
        Lưu hồ sơ người dùng vào Redis.
        
        Args:
            cc_num: Credit card number
            profile: Profile data (dob, gender, state, etc)
            ttl: Time to live in seconds
        """
        try:
            key = f"user:{cc_num}:profile"
            self.redis_client.setex(
                key,
                ttl,
                json.dumps(profile, ensure_ascii=False, default=str)
            )
            logger.debug(f"✅ User profile cached: {cc_num}")
        except Exception as e:
            logger.error(f"❌ Error setting user profile: {str(e)}")
    
    # ==================== TRANSACTION HISTORY ====================
    
    def get_user_history(self, cc_num: str, period: str = '24h') -> Optional[Dict[str, Any]]:
        """
        Lấy lịch sử giao dịch của user.
        
        Key:
            - user:{cc_num}:history:24h
            - user:{cc_num}:history:7d
        
        Returns:
            {
                "count": 5,
                "total_amt": 1000000,
                "avg_amt": 200000,
                "merchants": ["Merchant1", "Merchant2"],
                "last_transaction": "2024-05-12 10:30:00"
            }
        """
        try:
            data = self.redis_client.get(f"user:{cc_num}:history:{period}")
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"❌ Error getting user history: {str(e)}")
            return None
    
    def set_user_history(self, cc_num: str, period: str, history: Dict[str, Any], ttl: int = 3600):
        """
        Lưu lịch sử giao dịch vào Redis.
        
        TTL defaults:
        - 24h: 3600s (1 hour - updated hourly)
        - 7d: 86400s (24 hours - updated daily)
        """
        try:
            key = f"user:{cc_num}:history:{period}"
            self.redis_client.setex(
                key,
                ttl,
                json.dumps(history, ensure_ascii=False, default=str)
            )
            logger.debug(f"✅ User history cached: {cc_num} ({period})")
        except Exception as e:
            logger.error(f"❌ Error setting user history: {str(e)}")
    
    # ==================== LOCATION HISTORY ====================
    
    def add_location_history(self, cc_num: str, lat: float, long: float, 
                            timestamp: str, max_records: int = 50):
        """
        Thêm vị trí giao dịch vào location history.
        
        Key: user:{cc_num}:locations
        Format: JSON list được lưu dưới dạng Redis list
        Giữ lại 50 giao dịch gần nhất
        TTL: 604800s (7 days)
        """
        try:
            key = f"user:{cc_num}:locations"
            location_data = json.dumps({
                "lat": lat,
                "long": long,
                "timestamp": timestamp
            })
            
            # LPUSH to maintain time order
            self.redis_client.lpush(key, location_data)
            
            # Keep only last N records
            self.redis_client.ltrim(key, 0, max_records - 1)
            
            # Set TTL
            self.redis_client.expire(key, 604800)  # 7 days
            
            logger.debug(f"✅ Location added: {cc_num} ({lat}, {long})")
        except Exception as e:
            logger.error(f"❌ Error adding location: {str(e)}")
    
    def get_location_history(self, cc_num: str, count: int = 10) -> List[Dict[str, Any]]:
        """
        Lấy N vị trí giao dịch gần nhất của user.
        """
        try:
            key = f"user:{cc_num}:locations"
            locations = self.redis_client.lrange(key, 0, count - 1)
            return [json.loads(loc) for loc in locations]
        except Exception as e:
            logger.error(f"❌ Error getting location history: {str(e)}")
            return []
    
    # ==================== MERCHANT INFO ====================
    
    def get_merchant_info(self, merchant_id: str) -> Optional[Dict[str, Any]]:
        """
        Lấy thông tin merchant từ Redis.
        
        Key: merchant:{merchant_id}:info
        TTL: 86400s (24 hours)
        """
        try:
            data = self.redis_client.get(f"merchant:{merchant_id}:info")
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"❌ Error getting merchant info: {str(e)}")
            return None
    
    def set_merchant_info(self, merchant_id: str, info: Dict[str, Any], ttl: int = 86400):
        """Lưu thông tin merchant vào Redis."""
        try:
            key = f"merchant:{merchant_id}:info"
            self.redis_client.setex(
                key,
                ttl,
                json.dumps(info, ensure_ascii=False, default=str)
            )
        except Exception as e:
            logger.error(f"❌ Error setting merchant info: {str(e)}")
    
    # ==================== BLACKLIST / WHITELIST ====================
    
    def is_user_blacklisted(self, cc_num: str) -> bool:
        """Check if user is in blacklist (fraud accounts)."""
        try:
            return self.redis_client.sismember('fraud_blacklist:users', cc_num)
        except Exception as e:
            logger.error(f"❌ Error checking blacklist: {str(e)}")
            return False
    
    def add_to_blacklist(self, cc_num: str, reason: str = "", duration: int = 2592000):
        """
        Thêm user vào blacklist.
        
        TTL: 2592000s (30 days) - automatic removal
        """
        try:
            self.redis_client.sadd('fraud_blacklist:users', cc_num)
            # Log reason
            self.redis_client.hset(
                f"blacklist:{cc_num}",
                mapping={"reason": reason, "timestamp": datetime.utcnow().isoformat()}
            )
            self.redis_client.expire(f"blacklist:{cc_num}", duration)
            logger.warning(f"⚠️  User blacklisted: {cc_num} ({reason})")
        except Exception as e:
            logger.error(f"❌ Error adding to blacklist: {str(e)}")
    
    def is_merchant_whitelisted(self, merchant_id: str) -> bool:
        """Check if merchant is trusted (whitelist)."""
        try:
            return self.redis_client.sismember('fraud_whitelist:merchants', merchant_id)
        except Exception as e:
            logger.error(f"❌ Error checking whitelist: {str(e)}")
            return False
    
    # ==================== CACHE MANAGEMENT ====================
    
    def clear_user_cache(self, cc_num: str):
        """Xóa tất cả cache của 1 user (khi update profile)."""
        try:
            keys_to_delete = [
                f"user:{cc_num}:profile",
                f"user:{cc_num}:history:24h",
                f"user:{cc_num}:history:7d",
                f"user:{cc_num}:locations",
            ]
            self.redis_client.delete(*keys_to_delete)
            logger.info(f"✅ User cache cleared: {cc_num}")
        except Exception as e:
            logger.error(f"❌ Error clearing cache: {str(e)}")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Lấy thống kê Redis cache."""
        try:
            info = self.redis_client.info('stats')
            memory = self.redis_client.info('memory')
            
            return {
                "total_connections_received": info.get('total_connections_received', 0),
                "total_commands_processed": info.get('total_commands_processed', 0),
                "used_memory_human": memory.get('used_memory_human', 'N/A'),
                "used_memory_peak_human": memory.get('used_memory_peak_human', 'N/A'),
                "evicted_keys": info.get('evicted_keys', 0),
            }
        except Exception as e:
            logger.error(f"❌ Error getting cache stats: {str(e)}")
            return {}
    
    def close(self):
        """Close Redis connection."""
        try:
            self.redis_client.close()
            logger.info("✅ Redis connection closed")
        except Exception as e:
            logger.error(f"❌ Error closing Redis: {str(e)}")


# ==================== UTILITIES ====================

def create_user_profile_from_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Extract user profile từ transaction event."""
    return {
        "dob": event.get('dob'),
        "gender": event.get('gender'),
        "state": event.get('state'),
        "city": event.get('city', ''),
        "zip": event.get('zip', ''),
    }


def create_merchant_info_from_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Extract merchant info từ transaction event."""
    return {
        "merchant_id": event.get('merchant_id'),
        "merchant": event.get('merchant'),
        "category": event.get('category'),
        "lat": event.get('merch_lat'),
        "long": event.get('merch_long'),
        "city_pop": event.get('city_pop'),
    }


if __name__ == '__main__':
    # Test
    logging.basicConfig(level=logging.DEBUG)
    
    store = RedisFeatureStore(host='localhost')
    
    # Test set/get
    store.set_user_profile('4532015112830366', {
        'dob': '1990-01-01',
        'gender': 'M',
        'state': 'NY'
    })
    
    profile = store.get_user_profile('4532015112830366')
    print(f"Profile: {profile}")
    
    # Test location history
    store.add_location_history('4532015112830366', 40.7128, -74.0060, '2024-05-12 10:30:00')
    locations = store.get_location_history('4532015112830366')
    print(f"Locations: {locations}")
    
    # Test stats
    stats = store.get_cache_stats()
    print(f"Cache stats: {stats}")
    
    store.close()
