"""
☁️ MINIO DATA LAKE CONNECTOR
Lưu trữ raw + processed data cho audit trail và ML retraining
"""

import json
import logging
from typing import Dict, Any, Optional
from io import BytesIO
from datetime import datetime
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


class MinIODataLake:
    """MinIO connector for fraud detection data lake."""
    
    def __init__(self, 
                 endpoint: str = 'minio.storage.svc.cluster.local:9000',
                 access_key: str = 'minioadmin',
                 secret_key: str = 'minioadmin',
                 bucket_name: str = 'fraud-detection-lake',
                 secure: bool = False):
        """
        Khởi tạo MinIO client.
        
        Args:
            endpoint: MinIO server endpoint
            access_key: Access key
            secret_key: Secret key
            bucket_name: Bucket name
            secure: Use HTTPS
        """
        try:
            self.client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure
            )
            self.bucket_name = bucket_name
            
            # Create bucket if not exists
            self._ensure_bucket()
            
            logger.info(f"✅ MinIO connected: {endpoint}")
        except Exception as e:
            logger.error(f"❌ MinIO connection failed: {str(e)}")
            raise
    
    def _ensure_bucket(self):
        """Create bucket if it doesn't exist."""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"✅ Bucket created: {self.bucket_name}")
        except S3Error as e:
            if e.code == 'NoSuchBucket':
                # Try to create
                self.client.make_bucket(self.bucket_name)
                logger.info(f"✅ Bucket created: {self.bucket_name}")
            else:
                logger.error(f"❌ Error ensuring bucket: {str(e)}")
    
    # ==================== RAW DATA ====================
    
    def save_raw_transaction(self, transaction: Dict[str, Any]) -> bool:
        """
        Lưu raw transaction vào MinIO.
        
        Path: raw/transactions/{date}/{cc_num}_{timestamp}.json
        """
        try:
            trans_time = datetime.fromisoformat(
                transaction.get('trans_date_trans_time', datetime.utcnow().isoformat())
            )
            date_str = trans_time.strftime('%Y-%m-%d')
            timestamp_str = trans_time.strftime('%Y%m%d_%H%M%S_%f')
            cc_num = transaction.get('cc_num', 'unknown')
            
            # Anonymize cc_num in path (for compliance)
            cc_masked = f"{cc_num[:4]}****{cc_num[-4:]}" if len(cc_num) >= 8 else 'unknown'
            
            path = f"raw/transactions/{date_str}/{cc_masked}_{timestamp_str}.json"
            
            # Prepare data
            data = json.dumps(transaction, ensure_ascii=False, default=str).encode('utf-8')
            
            # Upload
            self.client.put_object(
                self.bucket_name,
                path,
                BytesIO(data),
                len(data),
                content_type='application/json'
            )
            
            logger.debug(f"✅ Raw transaction saved: {path}")
            return True
        
        except Exception as e:
            logger.error(f"❌ Error saving raw transaction: {str(e)}")
            return False
    
    # ==================== PROCESSED DATA ====================
    
    def save_processed_transaction(self, result: Dict[str, Any]) -> bool:
        """
        Lưu processed transaction (after fraud detection).
        
        Path: processed/transactions/{date}/{cc_num}_{timestamp}.json
        """
        try:
            timestamp_str = result.get('timestamp', datetime.utcnow().isoformat())
            trans_time = datetime.fromisoformat(timestamp_str)
            date_str = trans_time.strftime('%Y-%m-%d')
            timestamp_formatted = trans_time.strftime('%Y%m%d_%H%M%S_%f')
            cc_num = result.get('cc_num', 'unknown')
            
            cc_masked = f"{cc_num[:4]}****{cc_num[-4:]}" if len(cc_num) >= 8 else 'unknown'
            
            path = f"processed/transactions/{date_str}/{cc_masked}_{timestamp_formatted}.json"
            
            # Prepare data
            data = json.dumps(result, ensure_ascii=False, default=str).encode('utf-8')
            
            # Upload
            self.client.put_object(
                self.bucket_name,
                path,
                BytesIO(data),
                len(data),
                content_type='application/json'
            )
            
            logger.debug(f"✅ Processed transaction saved: {path}")
            return True
        
        except Exception as e:
            logger.error(f"❌ Error saving processed transaction: {str(e)}")
            return False
    
    # ==================== FEATURES ====================
    
    def save_feature_vectors(self, cc_num: str, features: Dict[str, Any]) -> bool:
        """
        Lưu feature vectors để dùng cho model retraining.
        
        Path: features/vectors/{date}/{cc_num}_{timestamp}.json
        """
        try:
            date_str = datetime.utcnow().strftime('%Y-%m-%d')
            timestamp_str = datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')
            
            cc_masked = f"{cc_num[:4]}****{cc_num[-4:]}" if len(cc_num) >= 8 else 'unknown'
            path = f"features/vectors/{date_str}/{cc_masked}_{timestamp_str}.json"
            
            data = json.dumps(features, ensure_ascii=False, default=str).encode('utf-8')
            
            self.client.put_object(
                self.bucket_name,
                path,
                BytesIO(data),
                len(data),
                content_type='application/json'
            )
            
            logger.debug(f"✅ Feature vectors saved: {path}")
            return True
        
        except Exception as e:
            logger.error(f"❌ Error saving features: {str(e)}")
            return False
    
    # ==================== MODELS ====================
    
    def save_model_version(self, model_name: str, version: str, model_data: bytes) -> bool:
        """
        Lưu model version để versioning.
        
        Path: models/{model_name}/v{version}/model.pkl
        """
        try:
            path = f"models/{model_name}/v{version}/model.pkl"
            
            self.client.put_object(
                self.bucket_name,
                path,
                BytesIO(model_data),
                len(model_data),
                content_type='application/octet-stream'
            )
            
            logger.info(f"✅ Model saved: {path}")
            return True
        
        except Exception as e:
            logger.error(f"❌ Error saving model: {str(e)}")
            return False
    
    def get_model_version(self, model_name: str, version: str) -> Optional[bytes]:
        """
        Lấy model version từ MinIO.
        """
        try:
            path = f"models/{model_name}/v{version}/model.pkl"
            response = self.client.get_object(self.bucket_name, path)
            return response.read()
        
        except Exception as e:
            logger.error(f"❌ Error retrieving model: {str(e)}")
            return None
    
    # ==================== ANALYSIS ====================
    
    def save_daily_report(self, report_date: str, report_data: Dict[str, Any]) -> bool:
        """
        Lưu daily fraud analysis report.
        
        Path: analysis/daily-reports/{date}/fraud_summary.json
        """
        try:
            path = f"analysis/daily-reports/{report_date}/fraud_summary.json"
            
            data = json.dumps(report_data, ensure_ascii=False, default=str).encode('utf-8')
            
            self.client.put_object(
                self.bucket_name,
                path,
                BytesIO(data),
                len(data),
                content_type='application/json'
            )
            
            logger.info(f"✅ Daily report saved: {path}")
            return True
        
        except Exception as e:
            logger.error(f"❌ Error saving report: {str(e)}")
            return False
    
    def save_fraud_patterns(self, patterns_data: Dict[str, Any]) -> bool:
        """
        Lưu detected fraud patterns (insights).
        
        Path: analysis/fraud-patterns/{timestamp}/patterns.json
        """
        try:
            timestamp_str = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            path = f"analysis/fraud-patterns/{timestamp_str}/patterns.json"
            
            data = json.dumps(patterns_data, ensure_ascii=False, default=str).encode('utf-8')
            
            self.client.put_object(
                self.bucket_name,
                path,
                BytesIO(data),
                len(data),
                content_type='application/json'
            )
            
            logger.debug(f"✅ Fraud patterns saved: {path}")
            return True
        
        except Exception as e:
            logger.error(f"❌ Error saving patterns: {str(e)}")
            return False
    
    # ==================== AUDIT ====================
    
    def get_user_transaction_history(self, cc_num: str, date: str) -> list:
        """
        Retrieve all transactions for a user on a specific date (audit trail).
        
        Args:
            cc_num: Credit card number
            date: Date in YYYY-MM-DD format
        
        Returns:
            List of transaction data
        """
        try:
            cc_masked = f"{cc_num[:4]}****{cc_num[-4:]}" if len(cc_num) >= 8 else 'unknown'
            prefix = f"processed/transactions/{date}/{cc_masked}_"
            
            transactions = []
            objects = self.client.list_objects(self.bucket_name, prefix=prefix)
            
            for obj in objects:
                response = self.client.get_object(self.bucket_name, obj.object_name)
                data = json.loads(response.read().decode('utf-8'))
                transactions.append(data)
            
            logger.debug(f"✅ Retrieved {len(transactions)} transactions for {cc_num} on {date}")
            return transactions
        
        except Exception as e:
            logger.error(f"❌ Error retrieving history: {str(e)}")
            return []
    
    # ==================== LIFECYCLE ====================
    
    def set_lifecycle_policy(self) -> bool:
        """
        Set lifecycle policies:
        - Raw data: 30 days → delete
        - Processed: 1 year
        - Models: keep forever
        """
        try:
            # Note: Actual lifecycle policy implementation depends on MinIO configuration
            logger.info("⚠️  Lifecycle policies should be configured via MinIO admin console")
            # TODO: Implement via MinIO API if available
            return True
        
        except Exception as e:
            logger.error(f"❌ Error setting lifecycle: {str(e)}")
            return False
    
    # ==================== STATS ====================
    
    def get_bucket_stats(self) -> Dict[str, Any]:
        """Get bucket statistics."""
        try:
            total_objects = 0
            total_size = 0
            
            objects = self.client.list_objects(self.bucket_name, recursive=True)
            for obj in objects:
                total_objects += 1
                total_size += obj.size
            
            return {
                'bucket_name': self.bucket_name,
                'total_objects': total_objects,
                'total_size_bytes': total_size,
                'total_size_gb': round(total_size / (1024**3), 2)
            }
        
        except Exception as e:
            logger.error(f"❌ Error getting stats: {str(e)}")
            return {}


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    
    # Test (requires MinIO running locally)
    lake = MinIODataLake(
        endpoint='localhost:9000',
        access_key='minioadmin',
        secret_key='minioadmin'
    )
    
    # Test save
    test_transaction = {
        'cc_num': '4532015112830366',
        'amt': 123.45,
        'trans_date_trans_time': '2024-05-12T10:30:00',
        'merchant': 'Test Merchant'
    }
    
    lake.save_raw_transaction(test_transaction)
    
    test_result = {
        'cc_num': '4532015112830366',
        'is_fraud': 0,
        'action': 'ALLOW',
        'timestamp': '2024-05-12T10:30:00'
    }
    
    lake.save_processed_transaction(test_result)
    
    # Stats
    stats = lake.get_bucket_stats()
    print(f"Bucket stats: {stats}")
