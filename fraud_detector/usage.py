import json
# Import cái class AI mày vừa viết ở file kia
from fraud_detector import FraudDetector

print("===== BẬT HỆ THỐNG KIỂM SOÁT GIAN LẬN =====")
print("1. Đang khởi động não AI (Load Model & Encoders)...")

# Khởi tạo class
detector = FraudDetector(
    model_path='best_fraud_model_096.json', 
    encoder_path='label_encoders.json',
    threshold=0.2426 # Cái mốc "Max Ping" hôm nọ đấy
    # Về param threshold
    # Default: 0.5
    # Max F1: 0.6334
    # Max F2: 0.2426
)
print("[+] Não AI đã sẵn sàng!\n")

# =====================================================================
# CASE 1: KHÁCH HÀNG NGOAN HIỀN (Giao dịch bình thường)
# Đặc điểm: Mua bó rau, chai mắm ở tạp hóa, số tiền nhỏ, lịch sử tiêu xài đều đặn.
# =====================================================================
event_normal = {
    "trans_date_trans_time": "2024-05-09 08:30:00", # Mua lúc 8h rưỡi sáng
    "dob": "1990-12-01",
    "amt": 25.5, # Quẹt có 25 đô
    "lat": 40.7128, "long": -74.0060,
    "merch_lat": 40.7130, "merch_long": -74.0050, # Cửa hàng sát vách nhà
    "category": "grocery_pos", # Mua tạp hóa
    "gender": "F",
    "state": "NY",
    "city_pop": 8000000,
    "trans_count_24h": 2,  
    "amt_sum_24h": 150.0,  
    "trans_count_7d": 12,  
    "amt_sum_7d": 600.0    # Lịch sử tiêu tiền rất khiêm tốn
}

print(">> Đang kiểm tra CASE 1: Khách quẹt thẻ mua rau...")
result_1 = detector.process_and_predict(event_normal)
print(json.dumps(result_1, indent=4, ensure_ascii=False))
print("-" * 50)


# =====================================================================
# CASE 2: THẰNG ẤT Ơ LỪA ĐẢO (Giao dịch đáng ngờ)
# Đặc điểm: (Lấy trong file test)
# =====================================================================
event_fraud = {
    "trans_date_trans_time": "2020-06-26 23:18:46",
    "dob": "1982-02-08",
    "amt": 949.88,
    "lat": 41.55, "long": -87.4569,
    "merch_lat": 41.618135, "merch_long": -87.55474699999999,
    "category": "shopping_net",
    "gender": "M",
    "state": "IN",
    "city_pop": 23727,
    "trans_count_24h": 1, "amt_sum_24h": 949.88,
    "trans_count_7d": 1, "amt_sum_7d": 949.88
}

print(">> Đang kiểm tra CASE 2: Nó nên là fraud...")
result_2 = detector.process_and_predict(event_fraud)
print(json.dumps(result_2, indent=4, ensure_ascii=False))
print("-" * 50)