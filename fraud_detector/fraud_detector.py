import json
import pandas as pd
import numpy as np
import xgboost as xgb
from google import genai
from google.genai import types
import shap
import os

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
        
        # 4. KHỞI TẠO SHAP EXPLAINER (Load 1 lần duy nhất lúc bật server cho đỡ lag)
        self.explainer = shap.TreeExplainer(self.model)
        
        # 5. KHỞI TẠO NÃO GEMINI 3.1 FLASH-LITE
        # Backend nhớ nhét API Key vào biến môi trường GEMINI_API_KEY nhé
        try:
            self.llm_client = genai.Client() # Khởi tạo Client
            self.llm_model_name = 'gemini-3.1-flash-lite'
        except Exception as e:
            print(f"⚠️ Cảnh báo: Không khởi tạo được LLM Client. Check lại GEMINI_API_KEY. Lỗi: {e}")
            self.llm_client = None

    # Hàm gọi Gemini để chém gió
    def _generate_alert_message(self, prob, dist, amt, shap_reasons):
        if not self.llm_client:
            return "CẢNH BÁO: AI phát hiện lừa đảo nhưng module LLM đang lỗi kết nối!"
        # Format cái lý do từ SHAP cho con LLM dễ đọc
        reasons_text = ", ".join([f"{item['feature']} ({item['direction']})" for item in shap_reasons])
        
        # PROMPT CHUẨN ĐỂ ĐI DEMO
        prompt = f"""
        Bạn là một chuyên gia quản trị rủi ro ngân hàng. Một giao dịch có vẻ là LỪA ĐẢO vừa bị phát hiện.
        - Tỷ lệ lừa đảo: {round(prob * 100, 2)}%
        - Khoảng cách của cửa hàng so với nhà: {dist:.1f} km
        - Số tiền quẹt: {amt} USD
        - Dấu hiệu bất thường (Từ AI SHAP): {reasons_text}
        
        Dựa theo các dấu hiệu được phát hiện, hãy viết ĐÚNG 1 CÂU giải thích lý do chi tiết tại sao các dấu hiệu này lại tiềm ẩn nguy cơ một giao dịch lừa đảo một cách thật chuyên nghiệp, trực diện, đanh thép, nhưng tránh dùng từ ngữ quá hàn lâm nêu bật mức độ nghiêm trọng và yêu cầu khóa thẻ ngay lập tức. Không dài dòng, không chào hỏi.
        """
        try:
            response = self.llm_client.models.generate_content(
                model=self.llm_model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.2, # Để 0.2 cho nó nghiêm túc
                )
            )
            return response.text.strip()
        except Exception as e:
            return "Hệ thống AI phát hiện bất thường nghiêm trọng. Vui lòng kiểm tra thủ công!"

    def process_and_predict(self, raw_event):
        try:
            # --- A & B: Tiền xử lý (Giữ nguyên code cũ của mày) ---
            trans_time = pd.to_datetime(raw_event['trans_date_trans_time'])
            hour = trans_time.hour
            dayofweek = trans_time.dayofweek
            
            dob = pd.to_datetime(raw_event['dob'])
            age = (trans_time - dob).days // 365
            
            dist = np.sqrt((raw_event['lat'] - raw_event['merch_lat'])**2 + 
                           (raw_event['long'] - raw_event['merch_long'])**2)
            
            avg_7d = (raw_event['amt_sum_7d'] / (raw_event['trans_count_7d'] + 1e-5))
            amt_vs_avg_7d = raw_event['amt'] / (avg_7d + 1e-5)
            
            cat_encoded = self.encoders['category'].get(str(raw_event['category']), -1)
            gender_encoded = self.encoders['gender'].get(str(raw_event['gender']), -1)
            state_encoded = self.encoders['state'].get(str(raw_event['state']), -1)
            
            # --- C. GOM VÀO DATAFRAME ---
            processed_data = {
                'amt': raw_event['amt'], 'category': cat_encoded, 'gender': gender_encoded,
                'state': state_encoded, 'lat': raw_event['lat'], 'long': raw_event['long'],
                'city_pop': raw_event['city_pop'], 'merch_lat': raw_event['merch_lat'],
                'merch_long': raw_event['merch_long'], 'hour': hour, 'dayofweek': dayofweek,
                'age': age, 'distance': dist, 
                'trans_count_24h': raw_event.get('trans_count_24h', 1),
                'amt_sum_24h': raw_event.get('amt_sum_24h', raw_event['amt']),
                'trans_count_7d': raw_event.get('trans_count_7d', 1),
                'amt_sum_7d': raw_event.get('amt_sum_7d', raw_event['amt']),
                'amt_vs_avg_7d': amt_vs_avg_7d
            }
            
            df = pd.DataFrame([processed_data])[self.feature_names]
            dmatrix = xgb.DMatrix(df)
            
            # --- D. CHỐT KẾT QUẢ ---
            prob = float(self.model.predict(dmatrix)[0])
            is_fraud = int(prob >= self.threshold)
            
            # --- E. SHAP VÀO VIỆC (Chỉ giải thích nếu phát hiện trộm) ---
            explanation = []
            if is_fraud:
                # Tính SHAP values cho cái Dataframe này
                shap_values = self.explainer.shap_values(df)
                
                # Ghép tên cột với điểm SHAP tương ứng (Lấy dòng đầu tiên [0])
                contributions = list(zip(self.feature_names, shap_values[0]))
                
                # Sắp xếp để lấy ra những cột có ảnh hưởng lớn nhất (Trị tuyệt đối to nhất)
                sorted_contrib = sorted(contributions, key=lambda x: abs(x[1]), reverse=True)
                
                # Bóc ra Top 3 nguyên nhân chính tạo thành dạng JSON thân thiện
                for feature, impact in sorted_contrib[:3]:
                    # Format lại value gốc để hiển thị cho đẹp
                    raw_val = processed_data[feature]
                    if isinstance(raw_val, float): raw_val = round(raw_val, 2)
                    
                    explanation.append({
                        "feature": feature,
                        "value": raw_val,
                        "impact_score": round(float(impact), 4),
                        "direction": "TĂNG RỦI RO" if impact > 0 else "GIẢM RỦI RO"
                    })
                
                # 2. GỌI GEMINI DỊCH SHAP RA TIẾNG NGƯỜI
                # Truyền data cho con LLM nó viết báo cáo
                alert_msg = self._generate_alert_message(
                    prob=prob, 
                    dist=dist, 
                    amt=raw_event['amt'], 
                    shap_reasons=explanation
                )

            # Trả về cục JSON full topping cho bọn Flink
            return {
                "status": "SUCCESS",
                "fraud_score": prob,
                "is_fraud": is_fraud,
                "action": "BLOCK" if is_fraud else "ALLOW",
                "shap_explanation": explanation, # Cục này rỗng nếu là giao dịch sạch
                "llm_alert": alert_msg if is_fraud else "Giao dịch này hợp lệ",
            }
            
        except Exception as e:
            return {"status": "ERROR", "message": f"Code AI sập vì lỗi data: {str(e)}"}