import os
import json
import pandas as pd
from datetime import datetime
from minio import Minio
from sqlalchemy import create_engine
import requests
from io import BytesIO

# --- CẤU HÌNH ---
# (Lưu ý: Khi chạy Docker thực tế sẽ dùng biến môi trường, đây là hardcode để test)
DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS = "minio_admin"
MINIO_SECRET = "minio_password_secure_123"
BUCKET_NAME = "social-trend-lake"
YOUTUBE_API_KEY = "AIzaSyABBaCFAyZWeasi2ToIl2O-diAfxIWpbmY" # <--- KEY CỦA BẠN

def get_active_topics():
    """Lấy danh sách các topic đang Active từ Metadata DB"""
    try:
        engine = create_engine(DB_CONN)
        query = "SELECT topic_id, topic_name, keywords, is_client FROM topics WHERE status = 'Active'"
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df.to_dict('records')
    except Exception as e:
        print(f"⚠️ Không kết nối được DB (Có thể do chạy local không có Postgres): {e}")
        # Trả về dữ liệu giả lập để test local
        return [{"topic_id": 1, "topic_name": "Sơn Tùng MTP", "keywords": "Sơn Tùng MTP"}]

def crawl_youtube_data(keywords):
    """Gọi YouTube API để lấy dữ liệu Deep Learning (Full Description + Tags)"""
    print(f"🔎 Crawling YouTube for: {keywords}")
    search_url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        'part': 'snippet',
        'q': keywords,
        'type': 'video',
        'maxResults': 10,
        'key': YOUTUBE_API_KEY
    }
    
    try:
        response = requests.get(search_url, params=params)
        response.raise_for_status()
        items = response.json().get('items', [])
        
        if not items:
            return []

        # --- ĐOẠN CODE ĐÃ SỬA (LỌC AN TOÀN) ---
        video_ids = []
        for item in items:
            # Kiểm tra chắc chắn xem kết quả này có chứa videoId không
            if 'id' in item and 'videoId' in item['id']:
                video_ids.append(item['id']['videoId'])
        
        if not video_ids:
            print("⚠️ API trả về kết quả nhưng không tìm thấy videoId nào.")
            return []
        # -------------------------------------
        
        # Gọi API lần 2 để lấy chi tiết Statistics và Tags
        stats_url = "https://www.googleapis.com/youtube/v3/videos"
        stats_params = {
            'part': 'statistics,snippet', # Lấy snippet lần nữa ở đây để lấy full description và tags
            'id': ','.join(video_ids),
            'key': YOUTUBE_API_KEY
        }
        stats_response = requests.get(stats_url, params=stats_params)
        stats_data = stats_response.json().get('items', [])
        
        results = []
        for video in stats_data:
            snippet = video.get('snippet', {})
            statistics = video.get('statistics', {})

            results.append({
                "video_id": video['id'],
                "title": snippet.get('title'),
                "channel": snippet.get('channelTitle'),
                "published_at": snippet.get('publishedAt'),
                # --- UPGRADE FOR NLP ---
                "description": snippet.get('description', ''), # Mỏ vàng cho Keyword Extraction
                "tags": snippet.get('tags', []),               # Keyword chính chủ (cực quan trọng)
                # -----------------------
                "view_count": int(statistics.get('viewCount', 0)),
                "like_count": int(statistics.get('likeCount', 0)),
                "comment_count": int(statistics.get('commentCount', 0)),
                "platform": "youtube",
                "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        return results

    except Exception as e:
        print(f"❌ Error crawling YouTube: {e}")
        return []

def upload_to_minio(topic_id, data):
    """Lưu dữ liệu vào Data Lake (MinIO)"""
    if not data:
        return

    try:
        client = Minio(MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET, secure=False)
        
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)

        json_data = json.dumps(data, ensure_ascii=False).encode('utf-8')
        
        now = datetime.now()
        # Lưu file với timestamp chi tiết để tránh trùng lặp
        path = f"raw/music/topic_id={topic_id}/year={now.year}/month={now.month:02d}/day={now.day:02d}/youtube_{int(datetime.timestamp(now))}.json"
        
        client.put_object(BUCKET_NAME, path, BytesIO(json_data), len(json_data), content_type="application/json")
        print(f"✅ Uploaded to MinIO: {path}")
    except Exception as e:
        print(f"⚠️ Lỗi Upload MinIO (Có thể do chạy Local không thấy MinIO Docker): {e}")
        # Backup: Lưu ra file local để kiểm tra
        local_path = f"youtube_data_topic_{topic_id}.json"
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"💾 Đã lưu tạm ra file local: {local_path}")

def main():
    print("--- STARTING SENIOR INGESTION (YOUTUBE - NLP READY) ---")
    topics = get_active_topics()
    
    for topic in topics:
        data = crawl_youtube_data(topic['keywords'])
        if data:
            upload_to_minio(topic['topic_id'], data)
            print(f"✨ Processed topic: {topic['topic_name']} ({len(data)} videos)")
        else:
            print(f"⚠️ No data for: {topic['topic_name']}")

if __name__ == "__main__":
    main()