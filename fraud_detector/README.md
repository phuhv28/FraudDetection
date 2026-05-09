# 🚨 AI Fraud Detector Module 🚨

Đây là cục AI Inference đã train xong.

## 📂 Cấu trúc folder

* `best_fraud_model_096.json`: Não của AI (XGBoost Model). Đã train với điểm số vip pro Average Precision cuối cùng: 0.96174 **(tuyệt đối không sửa)**
* `fraud_detector.py`: File core. Chứa class `FraudDetector` bao thầu hết mọi việc từ bóc tách features, tính toán lịch sử, đến ra lệnh ALLOW/BLOCK.
* `label_encoders.json`: Từ điển dịch từ Chữ (Text) sang Số cho con AI nó hiểu (đại loại là 1 file mapping). 
* `ui_dropdowns.json`: Gói quà tặng kèm cho thằng code **Frontend**. Chứa các danh sách tĩnh (Category, Gender, State) để đắp vào UI (Dropdown).
* `event_template.json`: **[QUAN TRỌNG]** Input cho AI. Đây là form JSON chuẩn mà Flink phải chìa ra cho AI. Tuy input của AI không cần cc_num (số thẻ) nhưng trước bước này, để hệ thống trả về kết quả cho một số tham số (có note trong file này) cần cc_num
*Ngoài ra, vì input cho AI là toạ độ nên cần có bước convert từ địa chỉ nhập từ UI sang toạ độ, nếu ae kiếm được API của Google Map thì ngon, hoặc k thì cho UI nhập vào toạ độ của nhà và shop*
* `usage.py`: File code mẫu để test thử xem AI chạy có mượt không cũng như cách sử dụng.
* `requirements.txt`: Các thư viện Python cần thiết để chạy module này.

---

## ⚙️ Cài đặt (Setup)

Nhập vào terminal dòng lệnh này:
```bash
pip install -r requirements.txt