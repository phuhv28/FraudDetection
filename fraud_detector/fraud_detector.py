import json
import pandas as pd
import numpy as np
import xgboost as xgb

class FraudDetector:
    def __init__(self, model_path='best_fraud_model_096.json', encoder_path='label_encoders.json', threshold=0.2426):
        # 1. Load não AI
        self.model = xgb.Booster()
        self.model.load_model(model_path)
        self.threshold = threshold
        
        # 2. Load từ điển mã hóa chữ -> số
        with open(encoder_path, 'r') as f:
            self.encoders = json.load(f)
            
        # 3. THỨ TỰ CỘT NHƯ LÚC TRAIN
        self.feature_names = [
            'category', 'amt', 'gender', 'state', 'lat', 'long', 'city_pop', 
            'merch_lat', 'merch_long', 'hour', 'dayofweek', 'age', 
            'distance', 'trans_count_24h', 'amt_sum_24h', 
            'trans_count_7d', 'amt_sum_7d', 'amt_vs_avg_7d'
        ]

    def process_and_predict(self, raw_event):
        """
        raw_event: Là 1 cục dictionary chứa data thô do Flink/Backend truyền vào.
        """
        try:
            # --- A. TỰ ĐỘNG TÍNH TOÁN CÁC FEATURE RÂU RIA ---
            
            # 1. Bóc thời gian
            trans_time = pd.to_datetime(raw_event['trans_date_trans_time'])
            hour = trans_time.hour
            dayofweek = trans_time.dayofweek
            
            # 2. Tính tuổi
            dob = pd.to_datetime(raw_event['dob'])
            age = (trans_time - dob).days // 365
            
            # 3. Tính khoảng cách nhà - shop
            dist = np.sqrt((raw_event['lat'] - raw_event['merch_lat'])**2 + 
                           (raw_event['long'] - raw_event['merch_long'])**2)
            
            # 4. Tính tỷ lệ tiêu tiền khác thường (có cộng 1e-5 để không bị lỗi chia cho 0)
            avg_7d = (raw_event['amt_sum_7d'] / (raw_event['trans_count_7d'] + 1e-5))
            amt_vs_avg_7d = raw_event['amt'] / (avg_7d + 1e-5)
            
            # --- B. DỊCH CHỮ SANG SỐ BẰNG TỪ ĐIỂN ---
            # Dùng .get(..., -1) để lỡ hệ thống gửi lên 1 cái State/Category lạ hoắc thì gán -1 chứ không bị sập code
            cat_encoded = self.encoders['category'].get(str(raw_event['category']), -1)
            gender_encoded = self.encoders['gender'].get(str(raw_event['gender']), -1)
            state_encoded = self.encoders['state'].get(str(raw_event['state']), -1)
            
            # --- C. GOM VÀO DATAFRAME & LÊN THỚT ---
            processed_data = {
                'amt': raw_event['amt'],
                'category': cat_encoded,
                'gender': gender_encoded,
                'state': state_encoded,
                'lat': raw_event['lat'],
                'long': raw_event['long'],
                'city_pop': raw_event['city_pop'],
                'merch_lat': raw_event['merch_lat'],
                'merch_long': raw_event['merch_long'],
                'hour': hour,
                'dayofweek': dayofweek,
                'age': age,
                'distance': dist,
                'trans_count_24h': raw_event.get('trans_count_24h', 1),
                'amt_sum_24h': raw_event.get('amt_sum_24h', raw_event['amt']),
                'trans_count_7d': raw_event.get('trans_count_7d', 1),
                'amt_sum_7d': raw_event.get('amt_sum_7d', raw_event['amt']),
                'amt_vs_avg_7d': amt_vs_avg_7d
            }
            
            # Ép chuẩn thứ tự cột rồi nhét vào XGBoost
            df = pd.DataFrame([processed_data])[self.feature_names]
            dmatrix = xgb.DMatrix(df)
            
            prob = float(self.model.predict(dmatrix)[0])
            is_fraud = int(prob >= self.threshold)
            
            return {
                "status": "SUCCESS",
                "fraud_score": prob,
                "is_fraud": is_fraud,
                "action": "BLOCK" if is_fraud else "ALLOW"
            }
            
        except Exception as e:
            return {"status": "ERROR", "message": f"Code AI sập vì lỗi data: {str(e)}"}