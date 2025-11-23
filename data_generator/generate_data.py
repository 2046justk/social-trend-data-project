import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import uuid
import os 

fake = Faker('vi_VN') # Dùng tiếng Việt cho chân thực

# ==========================================
# 1. CONFIGURATION (Cấu hình tỷ lệ dữ liệu)
# ==========================================
NUM_RECORDS = 10000  # Demo trước 10k dòng, sau này tăng lên 10 triệu
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 3, 30)

# ==========================================
# 2. MODULE MUSIC GENERATOR
# ==========================================
def generate_music_data(num):
    print(f"Generating {num} Music records...")
    
    # Danh sách Topic giả định
    topics = [
        {"id": "T001", "name": "Bài hát A (Vie Channel)", "is_client": True},
        {"id": "T002", "name": "Bài hát B (Indie)", "is_client": False},
        {"id": "T003", "name": "Show Rap Việt (Vie Channel)", "is_client": True},
        {"id": "T004", "name": "Sơn Tùng MTP", "is_client": False},
    ]
    
    data = []
    for _ in range(num):
        # Chọn topic: Client topic xuất hiện nhiều hơn (Bias volume)
        # Trọng số: Client (30%), Organic (20%) -> Giả lập Client được nhắc nhiều
        topic = random.choices(topics, weights=[0.3, 0.2, 0.3, 0.2], k=1)[0]
        
        # Giả lập Interaction: Nếu là Client, boost interaction lên
        base_interaction = random.randint(100, 1000)
        if topic["is_client"]:
            interaction = base_interaction * random.uniform(1.5, 3.0) # Boost 1.5x - 3x
        else:
            interaction = base_interaction

        record = {
            "id": str(uuid.uuid4()),
            "topic_id": topic["id"],
            "topic_name": topic["name"],
            "is_client": topic["is_client"],
            "platform": random.choice(['Facebook', 'TikTok', 'YouTube']),
            "content": fake.sentence(),
            "interaction_count": int(interaction),
            "timestamp": fake.date_time_between(start_date=START_DATE, end_date=END_DATE)
        }
        data.append(record)
    
    return pd.DataFrame(data)

# ==========================================
# 3. MODULE TRAVEL GENERATOR
# ==========================================
def generate_travel_data(num):
    print(f"Generating {num} Travel records...")
    
    locations = ["Núi Bà Đen", "Vũng Tàu", "Đà Lạt", "Phú Quốc"]
    spam_keywords = ["liên hệ", "giá vé", "tour trọn gói", "xe đưa đón", "inbox giá", "zalo"]
    
    data = []
    for _ in range(num):
        loc = random.choice(locations)
        
        # Giả lập Spam: Núi Bà Đen có 80% là bài bán tour
        is_commercial = False
        if loc == "Núi Bà Đen" and random.random() < 0.8:
            is_commercial = True
        elif random.random() < 0.2: # Các nơi khác chỉ 20% spam
            is_commercial = True
            
        if is_commercial:
            content = f"Tour {loc} {random.choice(spam_keywords)} {fake.phone_number()}"
        else:
            content = f"Hôm nay đi {loc} vui quá, cảnh đẹp tuyệt vời."

        record = {
            "id": str(uuid.uuid4()),
            "location": loc,
            "content": content,
            "author_id": fake.user_name(), # Để sau này detect spammer
            "platform": "Facebook",
            "timestamp": fake.date_time_between(start_date=START_DATE, end_date=END_DATE)
        }
        data.append(record)
        
    return pd.DataFrame(data)

# ==========================================
# 4. MODULE MOVIE GENERATOR
# ==========================================
def generate_movie_data(num_buzz, num_movies=10):
    print(f"Generating Movie Metadata & {num_buzz} Buzz records...")

    # 1. Tạo Metadata phim (Giả lập dữ liệu từ IMDB/CGV)
    movie_titles = [
        "Mai", "Đào, Phở và Piano", "Gặp Lại Chị Bầu", 
        "Dune: Part Two", "Kung Fu Panda 4", "Godzilla x Kong", 
        "Exhuma: Quật Mộ Trùng Ma", "Bố Già 2", "Lật Mặt 7", "Móng Vuốt"
    ]
    
    movies_meta = []
    # Giả lập lịch chiếu tập trung vào dịp Tết (Tháng 2) và Tháng 3
    base_release_date = datetime(2024, 2, 10) 
    
    for i, title in enumerate(movie_titles):
        # Random ngày chiếu rải rác từ tháng 1 đến tháng 4
        release_date = base_release_date + timedelta(days=random.randint(-30, 60))
        movies_meta.append({
            "movie_id": f"M{i+1:03d}",
            "movie_name": title,
            "release_date": release_date.strftime("%Y-%m-%d"),
            "status": "Coming Soon" if release_date > datetime.now() else "Released"
        })
    
    df_meta = pd.DataFrame(movies_meta)
    
    # 2. Tạo Buzz Data (Dữ liệu thảo luận)
    buzz_data = []
    
    for _ in range(num_buzz):
        # Chọn ngẫu nhiên 1 phim
        movie = random.choice(movies_meta)
        m_release = datetime.strptime(movie["release_date"], "%Y-%m-%d")
        
        # Logic sinh Buzz theo Lifecycle:
        # - Phase 1: Teaser (-30 ngày): Có Spike nhỏ
        # - Phase 2: Premiere (+/- 7 ngày): Spike cực lớn (Golden Time)
        # - Phase 3: Normal: Buzz thấp
        
        rand_val = random.random()
        
        if rand_val < 0.4: # 40% dữ liệu rơi vào dịp công chiếu (Premiere Hype)
            # Random ngày trong khoảng +/- 7 ngày so với Release Date
            post_date = m_release + timedelta(days=random.randint(-7, 7))
            interaction = random.randint(5000, 20000) # Buzz khủng
            content = f"Review phim {movie['movie_name']} vừa xem rạp xong, quá đỉnh!"
            
        elif rand_val < 0.6: # 20% dữ liệu rơi vào dịp tung Trailer (-30 ngày)
            post_date = m_release - timedelta(days=random.randint(25, 35))
            interaction = random.randint(1000, 5000) # Buzz khá
            content = f"Trailer mới của {movie['movie_name']} nhìn hứa hẹn phết"
            
        else: # 40% là thảo luận rải rác (Long-tail hoặc tin đồn)
            post_date = fake.date_time_between(start_date=START_DATE, end_date=END_DATE)
            interaction = random.randint(10, 500) # Buzz thấp
            content = f"Hóng phim {movie['movie_name']} quá"

        # Đảm bảo ngày post nằm trong khung thời gian chung của dataset
        if not (START_DATE <= post_date <= END_DATE):
            post_date = fake.date_time_between(start_date=START_DATE, end_date=END_DATE)

        record = {
            "id": str(uuid.uuid4()),
            "movie_id": movie["movie_id"],
            "movie_name": movie["movie_name"],
            "platform": random.choice(['Facebook', 'TikTok', 'YouTube']),
            "content": content,
            "interaction_count": interaction,
            "timestamp": post_date
        }
        buzz_data.append(record)

    return df_meta, pd.DataFrame(buzz_data)

# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    # Lấy đường dẫn của thư mục chứa file code này (data_generator)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    print(f"Saving files to: {current_dir}")

    # 1. Generate Music Data
    df_music = generate_music_data(NUM_RECORDS)
    # Dùng os.path.join để ghép đường dẫn chuẩn + dùng utf-8-sig để fix font
    df_music.to_csv(os.path.join(current_dir, "music_data_raw.csv"), index=False, encoding='utf-8-sig')
    print("Saved music_data_raw.csv")
    
    # 2. Generate Travel Data
    df_travel = generate_travel_data(NUM_RECORDS)
    df_travel.to_csv(os.path.join(current_dir, "travel_data_raw.csv"), index=False, encoding='utf-8-sig')
    print("Saved travel_data_raw.csv")
        
    # 3. Generate Movie Data
    df_movie_meta, df_movie_buzz = generate_movie_data(NUM_RECORDS)
    df_movie_meta.to_csv(os.path.join(current_dir, "movies_metadata.csv"), index=False, encoding='utf-8-sig')
    df_movie_buzz.to_csv(os.path.join(current_dir, "movies_buzz_raw.csv"), index=False, encoding='utf-8-sig')
    print("Saved movies_metadata.csv & movies_buzz_raw.csv")
    
    print("Done generation! Time to sleep 😴")