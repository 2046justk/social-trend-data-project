# 🚀 SocialTrend V2.0: Data-Driven Social Ranking Ecosystem

![Version](https://img.shields.io/badge/version-2.0-blue.svg)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Spark](https://img.shields.io/badge/Apache_Spark-3.5-orange)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.8-blue)
![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-red)

## 📖 Tổng quan dự án (Project Overview)
SocialTrend V2.0 là một hệ thống **Data Pipeline & Analytics** toàn diện, được thiết kế để giải quyết những "nỗi đau" (pain points) thực tế trong việc xếp hạng các xu hướng trên mạng xã hội. 

Thay vì chỉ đếm lượt tương tác (Buzz Volume) một cách mù quáng và thủ công, hệ thống sử dụng kiến trúc **Data Lakehouse**, kết hợp sức mạnh xử lý phân tán của **Apache Spark** và **NLP (Xử lý ngôn ngữ tự nhiên)** để mang lại một bảng xếp hạng **CÔNG BẰNG, CHÍNH XÁC VÀ CÓ NGỮ CẢNH.**

## 🎯 Giải quyết 3 bài toán nghiệp vụ cốt lõi (Business Values)

### 1. Module Travel (Du lịch): Bài toán "True-View Ranking"
* **Nỗi đau:** Bảng xếp hạng bị thao túng bởi rác "Spam bán tour, giá rẻ" trên Facebook/TikTok.
* **Giải pháp Spark:** Xây dựng luồng đối chiếu chéo (Cross-validation). Dùng NLP/Regex để đập tan bài đăng Spam trên MXH, sau đó kết hợp với điểm Rating trải nghiệm thực tế từ **Google Maps** để tạo ra điểm số `True-View Score`.

### 2. Module Music (Âm nhạc): Bài toán "Lifecycle Fairness" (Sự công bằng vòng đời)
* **Nỗi đau:** Sự bất công giữa Topic Client (được ép "sống" trên bảng xếp hạng dù đã hết hot - "Zombie") và Topic Organic (bị hệ thống xóa quá sớm dù còn tiềm năng - "Premature Death").
* **Giải pháp Spark:** Tích hợp dữ liệu API YouTube (Views/Likes) + MXH. Đưa ra hệ thống Auto-Tagging: Tự động đánh dấu `[👻 ZOMBIE]` để ẩn các topic nguội, và kích hoạt `[⚡ AUTO-EXTENDED]` để cứu sống các topic Organic chất lượng.

### 3. Module Movie (Phim ảnh): Bài toán "Contextual Engine" (La bàn ngữ cảnh)
* **Nỗi đau:** Content Creator bị đánh lừa bởi độ "Hype ảo" của Trailer, dẫn đến việc sản xuất nội dung sai thời điểm (Trend hạ nhiệt ngay khi phim ra rạp).
* **Giải pháp Spark:** Kết hợp Metadata `release_date` (Ngày khởi chiếu) và phân loại ý định thảo luận (Hype vs Review). Gắn nhãn cảnh báo `[⚠️ ĐANG HYPE TRAILER - Ngắn hạn]` để bảo vệ người dùng cuối.

## 🏗 Kiến trúc Hệ thống (Architecture)

Hệ thống tuân thủ nghiêm ngặt kiến trúc **Medallion Architecture** (Bronze -> Silver -> Gold) trên nền tảng Data Lakehouse.

* **Ingestion Layer:** Apache Airflow điều phối các tiến trình cào dữ liệu (Selenium cho Google Maps, API/Mock cho MXH).
* **Storage Layer:** MinIO (S3-Compatible) đóng vai trò Data Lake.
* **Processing Layer:** Apache Spark đảm nhiệm việc Join, Clean, Aggregate và chạy thuật toán.
* **Serving Layer:** Streamlit Web UI để trực quan hóa dữ liệu (Storytelling).

## 🚀 Hướng dẫn triển khai (How to run)

### Bước 1: Khởi động hạ tầng Docker
```bash
docker-compose up -d
```
* **Airflow UI:** Truy cập `http://localhost:8080` để chạy các DAGs Ingestion.
* **MinIO Storage:** Truy cập `http://localhost:9001` (User: `minio_admin` / Pass: `minio_password_secure_123`).

### Bước 2: Chạy Luồng xử lý Data Processing (Apache Spark)
Mở Terminal, truy cập vào Container Spark Master:
```bash
docker exec -u 0 -it spark-master bash
```
Chạy lần lượt 3 lệnh Submit tương ứng cho 3 Module:

**1. Xử lý Module Music:**
```bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.18 \
  /opt/spark/work-dir/src/processing/music/process_music.py
```

**2. Xử lý Module Travel:**
```bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.18 \
  /opt/spark/work-dir/src/processing/travel/process_travel.py
```

**3. Xử lý Module Movie:**
```bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.18 \
  /opt/spark/work-dir/src/processing/movie/process_movie.py
```

### Bước 3: Khởi chạy Giao diện Web (Streamlit)
Mở một Terminal mới (bên ngoài Docker) và chạy:
```bash
streamlit run src/serving/app.py
```
* **Giao diện Dashboard:** Truy cập `http://localhost:8501`

---
*This project is developed as a comprehensive Data Engineering Portfolio, demonstrating end-to-end capabilities in designing, processing, and automating a Modern Data Stack for Social Media Analytics.*
