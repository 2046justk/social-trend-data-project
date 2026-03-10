import time
import json
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from urllib.parse import quote

# --- CẤU HÌNH ---
SEARCH_KEYWORDS = ["Sơn Tùng MTP", "Du lịch Vũng Tàu"]
COOKIE_FILE = "src/ingestion/facebook/facebook_cookies.json"

def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--start-maximized")
    # Tắt dòng "Chrome is being controlled by automated test software"
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # --- LƯU Ý QUAN TRỌNG VỀ ĐƯỜNG DẪN CHROME ---
    # Nếu máy bạn cần chỉ định đường dẫn Chrome như lúc làm TikTok, hãy bỏ comment dòng dưới:
    # chrome_options.binary_location = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    return driver

def load_cookies(driver, cookie_file):
    """Nạp cookie từ file vào trình duyệt để bỏ qua bước Login - Hỗ trợ mọi định dạng J2TEAM"""
    try:
        with open(cookie_file, 'r') as f:
            data = json.load(f)
        
        # --- XỬ LÝ SỰ KHÁC BIỆT CẤU TRÚC JSON ---
        cookies = []
        if isinstance(data, list):
            # Trường hợp 1: File chỉ chứa list cookie [...]
            cookies = data
        elif isinstance(data, dict) and 'cookies' in data:
            # Trường hợp 2 (Của bạn): File có dạng {"url":..., "cookies": [...]}
            cookies = data['cookies']
        else:
            print("⚠️ Cấu trúc file cookie không nhận diện được, thử nạp trực tiếp...")
            cookies = data if isinstance(data, list) else []

        # Selenium yêu cầu phải đang ở domain facebook.com mới được set cookie
        driver.get("https://www.facebook.com")
        
        print(f"🍪 Tìm thấy {len(cookies)} cookies. Đang nạp...")

        for cookie in cookies:
            # Chỉ lấy các trường quan trọng, bỏ các trường gây lỗi strict
            cookie_dict = {
                'name': cookie.get('name'),
                'value': cookie.get('value'),
                'domain': cookie.get('domain', '.facebook.com'),
                'path': cookie.get('path', '/')
            }
            # Thêm expiry nếu có
            if 'expirationDate' in cookie:
                cookie_dict['expiry'] = int(cookie['expirationDate'])
            
            try:
                driver.add_cookie(cookie_dict)
            except Exception as e:
                # Bỏ qua các cookie lỗi nhỏ không quan trọng
                pass 
        
        print("✅ Đã nạp Cookie xong! Refresh lại trang để đăng nhập...")
        driver.refresh() # F5 để nhận đăng nhập
        time.sleep(5)
        
        # Kiểm tra xem có login thành công không
        # Nếu vẫn thấy nút "Log In" hoặc ô nhập pass nghĩa là thất bại
        if "login" in driver.current_url or len(driver.find_elements(By.NAME, "login")) > 0:
            print("⚠️ CẢNH BÁO: Có thể Cookie đã hết hạn hoặc chưa đăng nhập được. Bot sẽ thử cào ở chế độ ẩn danh.")
        else:
            print("🎉 ĐĂNG NHẬP THÀNH CÔNG BẰNG COOKIE!")
            
    except Exception as e:
        print(f"❌ Lỗi nạp cookie: {e}")

def extract_posts(html_source, keyword):
    soup = BeautifulSoup(html_source, 'html.parser')
    results = []
    
    # --- CHIẾN THUẬT MỚI: QUÉT TEXT TRỰC TIẾP ---
    # Thay vì tìm khung bài viết, ta tìm thẳng các thẻ chứa nội dung văn bản (Caption)
    # Trên Facebook, nội dung status luôn nằm trong thẻ div có dir="auto" và style đặc trưng
    content_divs = soup.find_all('div', {'dir': 'auto'})
    
    print(f"   📊 Tìm thấy {len(content_divs)} đoạn văn bản tiềm năng...")

    seen_texts = set() # Dùng để lọc trùng lặp ngay lập tức

    for div in content_divs:
        try:
            raw_text = div.get_text(separator=" ", strip=True)
            
            # 1. Lọc rác (Filter Spam/Noise)
            # Bỏ qua nếu text quá ngắn hoặc trùng lặp
            if len(raw_text) < 30: continue # Status thường phải dài hơn 30 ký tự
            if raw_text in seen_texts: continue
            
            seen_texts.add(raw_text)

            # 2. Xử lý NLP sơ bộ
            hashtags = [word for word in raw_text.split() if word.startswith("#")]
            # Clean text: Bỏ hashtag, bỏ icon (cơ bản)
            clean_text = " ".join([word for word in raw_text.split() if not word.startswith("#")])

            # 3. Lấy Author (Cố gắng tìm ngược lên trên)
            # Vì ta đang đứng ở text, việc tìm Author chính xác hơi khó, tạm thời để Unknown
            # hoặc logic phức tạp hơn (sẽ làm ở V2.1)
            author = "Facebook User"

            record = {
                "platform": "facebook",
                "search_keyword": keyword,
                "author": author,
                "caption_raw": raw_text,
                "caption_nlp": clean_text,
                "hashtags": hashtags,
                "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            results.append(record)
            
        except Exception:
            continue
    
    # Lấy tối đa 10-15 kết quả tốt nhất mỗi lần scroll để tránh rác
    return results[:15]

def main():
    print("🚀 BẮT ĐẦU CRAWL FACEBOOK (COOKIE INJECTION MODE)...")
    driver = get_driver()
    final_data = []

    try:
        # Bước 1: Login bằng Cookie
        load_cookies(driver, COOKIE_FILE)
        
        # Bước 2: Bắt đầu tìm kiếm
        for keyword in SEARCH_KEYWORDS:
            print(f"\n🔎 Đang tìm kiếm: '{keyword}'")
            encoded_query = quote(keyword)
            # URL tìm kiếm bài viết (Posts) công khai
            url = f"https://www.facebook.com/search/posts/?q={encoded_query}"
            
            driver.get(url)
            time.sleep(5)
            
            # Scroll giả lập người dùng
            for i in range(3):
                print(f"   ⬇️ Đang lướt xuống... ({i+1}/3)")
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(random.randint(4, 7)) 
            
            # Parse HTML
            html = driver.page_source
            data = extract_posts(html, keyword)
            final_data.extend(data)
            
            print(f"   ✅ Lấy được {len(data)} bài viết.")
            time.sleep(random.randint(5, 10))

    except Exception as e:
        print(f"❌ Lỗi: {e}")
    finally:
        print("🛑 Đóng trình duyệt.")
        driver.quit()

    output_file = "facebook_raw_data.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)
    
    print(f"\n💾 Đã lưu dữ liệu vào '{output_file}'")

if __name__ == "__main__":
    main()