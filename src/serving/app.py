import streamlit as st
import pandas as pd
import plotly.express as px

# --- CẤU HÌNH TRANG ---
st.set_page_config(page_title="Buzzmetrics - SocialTrend V2.0", page_icon="📈", layout="wide")

# --- KẾT NỐI MINIO LẤY GOLD DATA ---
STORAGE_OPTIONS = {
    "client_kwargs": {
        "endpoint_url": "http://localhost:9000", # <--- SỬA SỐ 1 THÀNH SỐ 0 Ở ĐÂY
        "aws_access_key_id": "minio_admin",
        "aws_secret_access_key": "minio_password_secure_123"
    }
}

@st.cache_data(ttl=60) # Cache data 60s cho mượt
def load_data(module_name):
    path = f"s3://social-trend-lake/processed/{module_name}_ranking/"
    try:
        return pd.read_parquet(path, storage_options=STORAGE_OPTIONS)
    except Exception as e:
        return pd.DataFrame()

# Tải dữ liệu
df_music = load_data("music")
df_travel = load_data("travel")
df_movie = load_data("movie")

# --- GIAO DIỆN CHÍNH ---
st.title("🚀 SOCIAL-TREND V2.0: AI & DATA-DRIVEN RANKING")
st.markdown("*Hệ thống Xếp hạng Tự động hóa tích hợp Data Engineering & NLP*")

tab_public, tab_admin = st.tabs(["🌍 GIAO DIỆN PUBLIC (END-USER)", "🕵️‍♂️ ADMIN DASHBOARD (BUZZMETRICS INTERNAL)"])

# ==========================================
# TAB 1: GIAO DIỆN PUBLIC (Đã được lọc sạch)
# ==========================================
with tab_public:
    st.header("🏆 BẢNG XẾP HẠNG THỰC TẾ (REAL-TIME TRENDS)")
    st.info("💡 Giao diện hiển thị cho người dùng. Đã loại bỏ rác Spam bán tour và ẩn các Topic Client ảo.")
    
    col1, col2, col3 = st.columns(3)
    
    # 1. MOVIE (Contextual Engine)
    with col1:
        st.subheader("🎬 Phim Ảnh (Movie)")
        if not df_movie.empty:
            df_movie_sorted = df_movie.sort_values(by="total_buzz", ascending=False).head(5)
            st.dataframe(
                df_movie_sorted[["topic_name", "total_buzz", "context_tag"]],
                column_config={"topic_name": "Tên Phim", "total_buzz": "Độ Hot", "context_tag": "Ngữ cảnh AI"},
                hide_index=True, use_container_width=True
            )
        else:
            st.warning("Đang chờ dữ liệu Spark...")

    # 2. TRAVEL (True-View Engine)
    with col2:
        st.subheader("✈️ Du Lịch (Travel)")
        if not df_travel.empty:
            df_travel_sorted = df_travel.sort_values(by="true_view_score", ascending=False).head(5)
            # Làm tròn điểm
            df_travel_sorted['true_view_score'] = df_travel_sorted['true_view_score'].round(0)
            st.dataframe(
                df_travel_sorted[["location_name", "avg_gmap_rating", "true_view_score"]],
                column_config={"location_name": "Địa điểm", "avg_gmap_rating": "GMaps Rating", "true_view_score": "Điểm True-View"},
                hide_index=True, use_container_width=True
            )

    # 3. MUSIC (Fairness Engine)
    with col3:
        st.subheader("🎵 Âm Nhạc (Music)")
        if not df_music.empty:
            # PUBLIC CHỈ SHOW BÀI HÁT HOT, ẨN ZOMBIE (Client nguội)
            df_music_public = df_music[~df_music["lifecycle_tag"].str.contains("ZOMBIE")]
            df_music_sorted = df_music_public.sort_values(by="total_buzz", ascending=False).head(5)
            st.dataframe(
                df_music_sorted[["topic_name", "total_buzz"]],
                column_config={"topic_name": "Bài hát", "total_buzz": "Tổng Lượt View & Buzz"},
                hide_index=True, use_container_width=True
            )

# ==========================================
# TAB 2: ADMIN DASHBOARD (Góc khuất Dữ liệu)
# ==========================================
with tab_admin:
    st.header("🛡️ HỆ THỐNG QUẢN TRỊ RỦI RO (DATA GOVERNANCE)")
    st.error("⚠️ CẢNH BÁO: Giao diện nội bộ. Nghiêm cấm chia sẻ ra ngoài.")
    
    # Kể câu chuyện Zombie vs Auto-Extend của Music
    st.subheader("1. Bài toán Công bằng Vòng đời (Music Module)")
    st.markdown("Hệ thống Spark đã tự động quét và phân loại tình trạng các Topic để ngăn chặn gian lận Bảng xếp hạng.")
    if not df_music.empty:
        # Vẽ biểu đồ trực quan
        fig = px.bar(
            df_music, x="topic_name", y="total_buzz", color="lifecycle_tag",
            title="Biểu đồ phân bổ Buzz Volume & Trạng thái Vòng đời",
            color_discrete_map={
                "[⚡ AUTO-EXTENDED - Organic Hot]": "green",
                "[🌟 CLIENT - Đang Hot]": "blue",
                "[👻 ZOMBIE - Client Ẩn]": "red"
            }
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Bảng chi tiết
        st.dataframe(df_music.sort_values("total_buzz", ascending=False), use_container_width=True)

    st.divider()
    
    # Kể câu chuyện Travel Spam
    st.subheader("2. Bài toán Trục lợi Spam Bán Tour (Travel Module)")
    st.markdown("Những địa điểm có Buzz ảo (cao) nhưng điểm trải nghiệm thật (Google Maps) thấp đã bị hệ thống True-View kéo tụt hạng.")
    if not df_travel.empty:
        df_travel_sorted = df_travel.sort_values(by="true_view_score", ascending=False)
        st.dataframe(df_travel_sorted, use_container_width=True)