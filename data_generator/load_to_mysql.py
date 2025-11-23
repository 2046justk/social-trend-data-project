import os
import pandas as pd
from sqlalchemy import create_engine, text

# 1. Cấu hình kết nối MySQL (Khớp với docker-compose.yml)
# User: user, Pass: password, Host: localhost, Port: 3306, DB: social_trend_db
db_connection_str = 'mysql+pymysql://user:password@localhost:3306/social_trend_db'
db_connection = create_engine(db_connection_str)

def load_csv_to_mysql(file_name, table_name):
    # Lấy đường dẫn file chuẩn (dùng os.path như lần trước để tránh lỗi không tìm thấy file)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, file_name)
    
    print(f"⏳ Dang doc file: {file_name}...")
    
    if not os.path.exists(file_path):
        print(f"❌ KHONG TIM THAY FILE: {file_path}")
        return

    try:
        # Đọc CSV
        df = pd.read_csv(file_path)
        
        # Ghi vào MySQL
        # if_exists='replace': Nếu bảng đã có thì xóa đi tạo lại (để reset dữ liệu sạch sẽ)
        print(f"🚀 Dang day {len(df)} dong vao bang '{table_name}'...")
        df.to_sql(name=table_name, con=db_connection, if_exists='replace', index=False)
        print(f"✅ Thanh cong! Table '{table_name}' da san sang.\n")
        
    except Exception as e:
        print(f"❌ Loi: {e}")

if __name__ == "__main__":
    print("--- BAT DAU NAP DU LIEU VAO MYSQL ---")
    
    # Test kết nối trước để đảm bảo Docker MySQL đang chạy
    try:
        with db_connection.connect() as connection:
            connection.execute(text("SELECT 1"))
        print("✅ Ket noi MySQL thanh cong!\n")
    except Exception as e:
        print(f"❌ Khong the ket noi MySQL. Hay kiem tra lai Docker! Loi: {e}")
        exit()

    # Nạp từng file vào từng bảng
    load_csv_to_mysql("music_data_raw.csv", "social_buzz_music")
    load_csv_to_mysql("travel_data_raw.csv", "social_buzz_travel")
    load_csv_to_mysql("movies_buzz_raw.csv", "social_buzz_movie")
    load_csv_to_mysql("movies_metadata.csv", "movies_metadata")
    
    print("--- HOAN TAT TOAN BO ---")