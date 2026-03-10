import time
import json
import random
from urllib.parse import quote # Thêm thư viện này để xử lý URL an toàn
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# --- CẤU HÌNH TỪ KHÓA ---
SEARCH_KEYWORDS = ["Sơn Tùng MTP", "Du lịch Vũng Tàu"]

def get_driver():
    """Khởi tạo Chrome Driver"""
    chrome_options = Options()
    # chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # --- LƯU Ý: Nếu máy bạn cần chỉ định đường dẫn Chrome thì bỏ comment dòng dưới ---
    # chrome_options.binary_location = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    return driver

def extract_data_from_html(html_source, keyword):
    soup = BeautifulSoup(html_source, 'html.parser')
    results = []
    
    # Tìm container video (Cập nhật class mới nhất hoặc tìm theo data-e2e)
    video_items = soup.find_all('div', {'data-e2e': 'search_top_item'}) 
    if not video_items:
        video_items = soup.select('div[class*="DivItemContainerForSearch"]')

    print(f"   📊 Tìm thấy {len(video_items)} video trên trang hiện tại...")

    for item in video_items:
        try:
            # 1. Lấy Caption Raw
            desc_tag = item.find('div', {'data-e2e': 'search-card-desc'})
            raw_caption = desc_tag.get_text(separator=" ", strip=True) if desc_tag else ""

            # 2. Tách Hashtag và Clean Text cho NLP (Logic quan trọng)
            hashtags = [word for word in raw_caption.split() if word.startswith("#")]
            # Loại bỏ hashtag để lấy văn bản thuần khiết cho NLP phân tích ngữ cảnh
            clean_text = " ".join([word for word in raw_caption.split() if not word.startswith("#")])

            # 3. Lấy Link & Author
            link_tag = item.find('a')
            video_link = link_tag['href'] if link_tag else ""
            author_tag = item.find('p', {'data-e2e': 'search-card-user-unique-id'})
            author = author_tag.text if author_tag else "Unknown"

            # 4. Lấy Likes
            like_tag = item.find('strong', {'data-e2e': 'search-card-like-container'})
            likes_str = like_tag.text if like_tag else "0"
            
            record = {
                "platform": "tiktok",
                "search_keyword": keyword,
                "author": author,
                "url": video_link,
                "caption_raw": raw_caption,
                "caption_nlp": clean_text,      # <-- TRƯỜNG MỚI QUAN TRỌNG
                "hashtags": hashtags,
                "likes_display": likes_str,
                "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            results.append(record)

        except Exception as e:
            continue
            
    return results

def main():
    print("🚀 BẮT ĐẦU CRAWL TIKTOK (VERSION 2 - NLP OPTIMIZED)...")
    driver = get_driver()
    final_data = []

    try:
        for keyword in SEARCH_KEYWORDS:
            print(f"\n🔎 Đang tìm kiếm: '{keyword}'")
            
            # Encode URL an toàn
            encoded_query = quote(keyword)
            url = f"https://www.tiktok.com/search?q={encoded_query}"
            
            driver.get(url)
            time.sleep(5) 
            
            # Scroll
            for i in range(3): 
                print(f"   ⬇️ Đang lướt xuống... ({i+1}/3)")
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(4) 
            
            # Parse
            html_source = driver.page_source
            data = extract_data_from_html(html_source, keyword)
            final_data.extend(data)
            
            print(f"   ✅ Lấy được {len(data)} video.")
            time.sleep(random.randint(5, 10))

    except Exception as e:
        print(f"❌ Lỗi: {e}")
    finally:
        driver.quit()

    output_file = "tiktok_raw_data.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)
    
    print(f"\n💾 Đã lưu dữ liệu mới vào '{output_file}'")

if __name__ == "__main__":
    main()