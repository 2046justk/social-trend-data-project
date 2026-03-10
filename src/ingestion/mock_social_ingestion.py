import os
import json
import random
import logging
from datetime import datetime
from minio import Minio
from sqlalchemy import create_engine
import pandas as pd
from io import BytesIO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS = "minio_admin"
MINIO_SECRET = "minio_password_secure_123"
BUCKET_NAME = "social-trend-lake"

def get_all_topics():
    engine = create_engine(DB_CONN)
    # Lấy cả topics (Music/Movie) và travel_locations
    query_topics = "SELECT topic_id as id, topic_name as name, 'topic' as type, release_date FROM topics WHERE status = 'Active'"
    query_travel = "SELECT location_id as id, location_name as name, 'location' as type, NULL as release_date FROM travel_locations WHERE status = 'Active'"
    
    with engine.connect() as conn:
        df_topics = pd.read_sql(query_topics, conn)
        df_travel = pd.read_sql(query_travel, conn)
        
    return pd.concat([df_topics, df_travel]).to_dict('records')

def generate_smart_mock_data(item):
    """Sinh dữ liệu giả lập nhưng CÓ MỤC ĐÍCH NGHIỆP VỤ RÕ RÀNG"""
    data = []
    platforms = ['facebook', 'tiktok']
    
    for platform in platforms:
        # Giả lập Buzz Volume (Độ ồn ào)
        buzz_count = random.randint(100, 5000)
        
        # Sinh text để lát nữa NLP xử lý
        comments = []
        if item['type'] == 'location':
            # Travel: Cố tình trộn rác Spam bán tour vào
            comments.append(f"Tour {item['name']} 3 ngày 2 đêm giá cực rẻ, liên hệ Zalo 09xx ngay!")
            comments.append(f"Ai đi {item['name']} rồi cho mình xin review với ạ.")
        elif item['type'] == 'topic' and item['release_date']:
            # Movie: Cố tình sinh text Hype hoặc Review dựa trên ngày
            now = datetime.now()
            if now < item['release_date']:
                comments.append(f"Hóng phim {item['name']} quá, trailer xịn xò ghê!") # Hype
            else:
                comments.append(f"Mới đi xem {item['name']} về, kết phim cảm động rơi nước mắt.") # Review
        else:
            # Music
            comments.append(f"Bài {item['name']} dạo này đi đâu cũng nghe.")
            
        data.append({
            "entity_id": item['id'],
            "entity_name": item['name'],
            "platform": platform,
            "buzz_volume": buzz_count,
            "sample_comments": comments,
            "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    return data

def upload_to_datalake(entity_id, entity_type, data):
    if not data: return
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
    if not client.bucket_exists(BUCKET_NAME): client.make_bucket(BUCKET_NAME)

    json_data = json.dumps(data, ensure_ascii=False).encode('utf-8')
    now = datetime.now()
    # Folder cấu trúc: raw/social/type=topic/entity_id=1/year=...
    path = f"raw/social/type={entity_type}/entity_id={entity_id}/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/data.json"
    
    client.put_object(BUCKET_NAME, path, BytesIO(json_data), len(json_data), content_type="application/json")
    logger.info(f"💾 Đã lưu Mock Social Data vào: s3://{BUCKET_NAME}/{path}")

def main():
    logger.info("🚀 BẮT ĐẦU CHẠY SMART MOCK INGESTION (FB/TIKTOK)")
    items = get_all_topics()
    for item in items:
        logger.info(f"Đang sinh dữ liệu FB/TikTok cho: {item['name']}")
        mock_data = generate_smart_mock_data(item)
        upload_to_datalake(item['id'], item['type'], mock_data)
    logger.info("🏁 KẾT THÚC.")

if __name__ == "__main__":
    main()