import os
import time
import json
import re
from datetime import datetime
from bs4 import BeautifulSoup
from minio import Minio
from io import BytesIO
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- CẤU HÌNH ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minio_password_secure_123")
BUCKET_NAME = "social-trend-lake"

def parse_money(money_str):
    try:
        money_str = money_str.lower().replace(',', '.').strip()
        number = float(re.findall(r"[-+]?\d*\.\d+|\d+", money_str)[0])
        if 'tỷ' in money_str: return int(number * 1_000_000_000)
        elif 'triệu' in money_str: return int(number * 1_000_000)
        elif 'k' in money_str: return int(number * 1000)
        else: return int(number)
    except:
        return 0

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    # Đổi User-Agent sang một trình duyệt thực tế phổ biến
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=chrome_options)

def crawl_bovn():
    print("🎬 Đang truy cập Box Office Vietnam bằng Selenium...")
    url = "https://boxofficevietnam.com/"
    driver = setup_driver()
    
    try:
        driver.get(url)
        
        # 1. KIỂM TRA XEM CÓ BỊ CLOUDFLARE CHẶN KHÔNG
        title = driver.title
        print(f"🌐 Tiêu đề trang hiện tại: '{title}'")
        if "Just a moment" in title or "Cloudflare" in title:
            print("🚨 CẢNH BÁO: Chúng ta đang bị hệ thống Anti-bot (Cloudflare) chặn ở ngoài cửa!")
            
        # 2. ĐỢI BẢNG XUẤT HIỆN (Tối đa 15 giây)
        print("⏳ Đang đợi bảng dữ liệu xuất hiện...")
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
        except:
            print("⚠️ Hết 15s chờ. Bảng vẫn chưa xuất hiện (Có thể do mạng chậm hoặc bị chặn).")

        # 3. CHỤP X-QUANG: Lưu lại toàn bộ mã nguồn HTML mà Bot nhìn thấy
        html_content = driver.page_source
        with open("/opt/airflow/src/ingestion/movie/debug_bovn.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        print("📝 Đã lưu file 'debug_bovn.html'. (Nếu fail, bạn có thể mở file này ra xem bot đang thấy gì).")

        # 4. TÌM BẢNG BẰNG MỌI GIÁ
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table')
        
        target_table = None
        for t in tables:
            # Tìm bảng nào có thẻ tbody và có nhiều hơn 2 dòng
            if t.find('tbody') and len(t.find('tbody').find_all('tr')) > 2:
                target_table = t
                break
                
        if not target_table:
            print("❌ Vẫn không tìm thấy bảng chứa dữ liệu phim!")
            return []

        rows = target_table.find('tbody').find_all('tr')
        results = []
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 5:
                movie_name = cols[1].text.strip()
                daily_revenue = cols[2].text.strip()
                screenings = cols[3].text.strip()
                total_revenue = cols[4].text.strip()
                
                # Bỏ qua dòng trống hoặc dòng header
                if movie_name == "" or "Phim" in movie_name:
                    continue

                results.append({
                    "movie_name": movie_name,
                    "daily_revenue_vnd": parse_money(daily_revenue),
                    "total_revenue_vnd": parse_money(total_revenue),
                    "screenings": int(re.sub(r'[^\d]', '', screenings)) if screenings else 0,
                    "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                
        print(f"✅ Đã cào thành công {len(results)} phim đang chiếu rạp.")
        return results

    except Exception as e:
        print(f"❌ Lỗi khi cào Box Office: {e}")
        return []
    finally:
        driver.quit()

def upload_to_minio(data):
    if not data:
        return
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET, secure=False)
    if not client.bucket_exists(BUCKET_NAME): 
        client.make_bucket(BUCKET_NAME)

    json_data = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
    now = datetime.now()
    path = f"raw/movie/boxoffice/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/data_{now.minute}.json"
    
    client.put_object(BUCKET_NAME, path, BytesIO(json_data), len(json_data), content_type="application/json")
    print(f"💾 Upload thành công lên Data Lake: s3://{BUCKET_NAME}/{path}")

def main():
    print("--- STARTING REAL INGESTION (MOVIE - BOX OFFICE) ---")
    data = crawl_bovn()
    if data:
        upload_to_minio(data)

if __name__ == "__main__":
    main()