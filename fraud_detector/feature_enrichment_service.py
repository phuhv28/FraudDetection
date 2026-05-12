"""
🔄 FEATURE ENRICHMENT SERVICE
Batch job để populate Redis với user features từ historical data
"""

import json
import logging
import asyncio
from typing import Dict, Any, List
from datetime import datetime, timedelta
import math

logger = logging.getLogger(__name__)


class FeatureEnrichmentService:
    """
    Batch service để enrich user profiles từ historical data.
    Chạy hourly để update Redis cache với:
    - User profiles (demographic)
    - Transaction history (24h, 7d)
    - Location history (last 50)
    - Merchant information
    """
    
    def __init__(self, 
                 redis_client,
                 minio_client=None,
                 db_connection=None):
        """
        Initialize enrichment service.
        
        Args:
            redis_client: Redis instance
            minio_client: MinIO instance (for reading historical data)
            db_connection: Database connection (for user demographics)
        """
        self.redis = redis_client
        self.minio = minio_client
        self.db = db_connection
    
    async def enrich_user_profile(self, cc_num: str) -> Dict[str, Any]:
        """
        Enrich single user profile từ database.
        Cache key: user:{cc_num}:profile
        """
        try:
            # Query database for user demographics
            user_profile = {
                'cc_num': cc_num,
                'dob': '1990-05-15',  # From database
                'gender': 'M',
                'state': 'CA',
                'city': 'San Francisco',
                'created_date': '2020-01-01',
                'account_age_days': 1500,
                'account_status': 'ACTIVE'
            }
            
            # Cache for 24 hours
            key = f"user:{cc_num}:profile"
            self.redis.setex(
                key,
                86400,  # 24 hours
                json.dumps(user_profile)
            )
            
            logger.debug(f"✅ User profile enriched: {cc_num}")
            return user_profile
        
        except Exception as e:
            logger.error(f"❌ Error enriching user profile: {str(e)}")
            return {}
    
    async def enrich_user_history_24h(self, cc_num: str) -> Dict[str, Any]:
        """
        Tính transaction history cho 24h qua.
        Cache key: user:{cc_num}:history:24h
        
        Returns:
            {
                'count': số giao dịch,
                'total_amt': tổng tiền,
                'avg_amt': trung bình,
                'std_dev_amt': độ lệch chuẩn,
                'merchants': danh sách merchant unique
            }
        """
        try:
            # Query từ MinIO hoặc database
            transactions = await self._fetch_transactions(cc_num, hours=24)
            
            if not transactions:
                history = {
                    'count': 0,
                    'total_amt': 0,
                    'avg_amt': 0,
                    'std_dev_amt': 0,
                    'merchants': []
                }
            else:
                amounts = [t.get('amt', 0) for t in transactions]
                merchants = list(set([t.get('merchant', 'Unknown') for t in transactions]))
                
                history = {
                    'count': len(transactions),
                    'total_amt': sum(amounts),
                    'avg_amt': sum(amounts) / len(amounts),
                    'std_dev_amt': self._calculate_std_dev(amounts),
                    'merchants': merchants
                }
            
            # Cache for 1 hour
            key = f"user:{cc_num}:history:24h"
            self.redis.setex(
                key,
                3600,  # 1 hour
                json.dumps(history)
            )
            
            logger.debug(f"✅ 24h history enriched: {cc_num} ({history['count']} trans)")
            return history
        
        except Exception as e:
            logger.error(f"❌ Error enriching 24h history: {str(e)}")
            return {
                'count': 0,
                'total_amt': 0,
                'avg_amt': 0,
                'std_dev_amt': 0,
                'merchants': []
            }
    
    async def enrich_user_history_7d(self, cc_num: str) -> Dict[str, Any]:
        """
        Tính transaction history cho 7 ngày qua.
        Cache key: user:{cc_num}:history:7d
        """
        try:
            transactions = await self._fetch_transactions(cc_num, hours=168)  # 7 days
            
            if not transactions:
                history = {
                    'count': 0,
                    'total_amt': 0,
                    'avg_amt': 0,
                    'std_dev_amt': 0,
                    'merchants': []
                }
            else:
                amounts = [t.get('amt', 0) for t in transactions]
                merchants = list(set([t.get('merchant', 'Unknown') for t in transactions]))
                
                history = {
                    'count': len(transactions),
                    'total_amt': sum(amounts),
                    'avg_amt': sum(amounts) / len(amounts),
                    'std_dev_amt': self._calculate_std_dev(amounts),
                    'merchants': merchants
                }
            
            # Cache for 24 hours
            key = f"user:{cc_num}:history:7d"
            self.redis.setex(
                key,
                86400,  # 24 hours
                json.dumps(history)
            )
            
            logger.debug(f"✅ 7d history enriched: {cc_num} ({history['count']} trans)")
            return history
        
        except Exception as e:
            logger.error(f"❌ Error enriching 7d history: {str(e)}")
            return {
                'count': 0,
                'total_amt': 0,
                'avg_amt': 0,
                'std_dev_amt': 0,
                'merchants': []
            }
    
    async def enrich_location_history(self, cc_num: str, max_records: int = 50) -> List[Dict]:
        """
        Populate location history từ recent transactions.
        Cache key: user:{cc_num}:locations (Redis list)
        
        Keeps last 50 locations with TTL of 7 days
        """
        try:
            transactions = await self._fetch_transactions(cc_num, hours=168, limit=100)
            
            locations = []
            for trans in transactions:
                location = {
                    'lat': trans.get('lat'),
                    'long': trans.get('long'),
                    'merchant': trans.get('merchant'),
                    'timestamp': trans.get('trans_date_trans_time')
                }
                if location['lat'] and location['long']:
                    locations.append(location)
            
            # Keep only last 50
            locations = locations[:max_records]
            
            # Store in Redis list
            key = f"user:{cc_num}:locations"
            
            # Clear existing
            self.redis.delete(key)
            
            # Add locations
            for loc in locations:
                self.redis.lpush(key, json.dumps(loc))
            
            # Set TTL
            self.redis.expire(key, 604800)  # 7 days
            
            logger.debug(f"✅ Location history enriched: {cc_num} ({len(locations)} locations)")
            return locations
        
        except Exception as e:
            logger.error(f"❌ Error enriching locations: {str(e)}")
            return []
    
    async def enrich_merchant_info(self, merchant_id: str) -> Dict[str, Any]:
        """
        Enrich merchant information từ database.
        Cache key: merchant:{merchant_id}:info
        """
        try:
            # Query database for merchant info
            merchant_info = {
                'merchant_id': merchant_id,
                'merchant': 'Example Store',
                'category': 'ELECTRONICS',
                'high_risk_flag': False,
                'avg_transaction': 5000,
                'chargeback_rate': 0.001
            }
            
            # Cache for 24 hours
            key = f"merchant:{merchant_id}:info"
            self.redis.setex(
                key,
                86400,  # 24 hours
                json.dumps(merchant_info)
            )
            
            logger.debug(f"✅ Merchant info enriched: {merchant_id}")
            return merchant_info
        
        except Exception as e:
            logger.error(f"❌ Error enriching merchant info: {str(e)}")
            return {}
    
    async def batch_enrich_users(self, user_list: List[str]) -> Dict[str, Any]:
        """
        Batch enrich multiple users (hourly batch job).
        
        Args:
            user_list: List of credit card numbers to enrich
        
        Returns:
            Summary statistics
        """
        logger.info(f"🔄 Starting batch enrichment for {len(user_list)} users")
        
        stats = {
            'total_users': len(user_list),
            'enriched': 0,
            'failed': 0,
            'start_time': datetime.utcnow().isoformat(),
            'end_time': None
        }
        
        try:
            for cc_num in user_list:
                try:
                    # Enrich all aspects
                    await self.enrich_user_profile(cc_num)
                    await self.enrich_user_history_24h(cc_num)
                    await self.enrich_user_history_7d(cc_num)
                    await self.enrich_location_history(cc_num)
                    
                    stats['enriched'] += 1
                
                except Exception as e:
                    logger.warning(f"⚠️  Failed to enrich {cc_num}: {str(e)}")
                    stats['failed'] += 1
            
            stats['end_time'] = datetime.utcnow().isoformat()
            duration = (datetime.fromisoformat(stats['end_time']) - 
                       datetime.fromisoformat(stats['start_time'])).total_seconds()
            stats['duration_seconds'] = round(duration, 2)
            
            logger.info(f"✅ Batch enrichment complete: {stats['enriched']}/{stats['total_users']} "
                       f"enriched in {stats['duration_seconds']}s")
            
            return stats
        
        except Exception as e:
            logger.error(f"❌ Error in batch enrichment: {str(e)}")
            stats['end_time'] = datetime.utcnow().isoformat()
            return stats
    
    # ==================== UTILITIES ====================
    
    async def _fetch_transactions(self, 
                                  cc_num: str, 
                                  hours: int = 24,
                                  limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Fetch transactions from MinIO or database.
        Placeholder - replace with actual data source.
        """
        # TODO: Implement data source query
        return []
    
    @staticmethod
    def _calculate_std_dev(values: List[float]) -> float:
        """Calculate standard deviation."""
        if not values or len(values) < 2:
            return 0.0
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return math.sqrt(variance)
    
    # ==================== HEALTH CHECK ====================
    
    def health_check(self) -> Dict[str, Any]:
        """Check service health."""
        try:
            # Test Redis
            self.redis.ping()
            redis_status = 'HEALTHY'
        except Exception as e:
            logger.error(f"❌ Redis health check failed: {str(e)}")
            redis_status = 'UNHEALTHY'
        
        return {
            'service': 'FeatureEnrichmentService',
            'status': 'HEALTHY' if redis_status == 'HEALTHY' else 'UNHEALTHY',
            'redis_status': redis_status,
            'timestamp': datetime.utcnow().isoformat()
        }


# ==================== HOURLY BATCH JOB ====================

async def hourly_enrichment_job(redis_client, user_list: List[str] = None):
    """
    Hourly batch job - triggered by Kubernetes CronJob.
    Enriches top active users from past day.
    """
    enrichment_service = FeatureEnrichmentService(redis_client)
    
    # If no user list provided, fetch top active users
    if user_list is None:
        # TODO: Query top active users from database
        user_list = ['4532015112830366', '4532015112830367']
    
    result = await enrichment_service.batch_enrich_users(user_list)
    return result


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    
    # Test (requires Redis running)
    import redis
    
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    enrichment = FeatureEnrichmentService(r)
    
    # Health check
    print(f"Health: {enrichment.health_check()}")
    
    # Test single user enrichment
    # asyncio.run(enrichment.enrich_user_profile('4532015112830366'))
