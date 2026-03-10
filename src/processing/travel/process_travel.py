import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when, avg, sum as _sum, explode, regexp_replace
from pyspark.sql.types import StringType

# --- CẤU HÌNH ---
MINIO_ENDPOINT = "http://minio:9000"
BUCKET_NAME = "social-trend-lake"
DB_CONN = "jdbc:postgresql://postgres:5432/airflow"
DB_PROPERTIES = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

def get_spark_session():
    return SparkSession.builder \
        .appName("Travel True-View Processing V2.1") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio_password_secure_123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    print("🚀 BẮT ĐẦU XỬ LÝ DỮ LIỆU TRAVEL (TRUE-VIEW ENGINE)...")

    # 1. ĐỌC METADATA TỪ POSTGRES
    df_locations = spark.read.jdbc(url=DB_CONN, table="travel_locations", properties=DB_PROPERTIES) \
                             .select(col("location_id").alias("id"), "location_name")

    # 2. NHÁNH 1: ĐỌC GOOGLE MAPS (Sửa lỗi Path và ID Mismatch)
    print("📍 Đang đọc dữ liệu Google Maps...")
    try:
        # FIX 1: Trỏ thẳng vào thư mục gốc, để Spark tự phân tích cấu trúc
        df_gmaps = spark.read.json(f"s3a://{BUCKET_NAME}/raw/travel/source=gmaps/")
        
        # FIX 2: Ép kiểu ID ("loc_001" -> 1) để Join với Postgres
        df_gmaps_clean = df_gmaps.withColumn("loc_id_int", regexp_replace(col("location_id"), "loc_00", "").cast("int"))
        
        # Tính trung bình Rating
        df_gmaps_agg = df_gmaps_clean.groupBy("loc_id_int").agg(avg("rating").alias("avg_gmap_rating"))
    except Exception as e:
        print(f"⚠️ Không tìm thấy dữ liệu Google Maps: {e}")
        df_gmaps_agg = spark.createDataFrame([], schema="loc_id_int INT, avg_gmap_rating DOUBLE")

    # 3. NHÁNH 2: ĐỌC FB/TIKTOK & LỌC SPAM BÁN TOUR
    print("📱 Đang đọc FB/TikTok và kích hoạt bộ lọc Anti-Spam...")
    try:
        # FIX 1: Trỏ thẳng vào thư mục gốc
        df_social_raw = spark.read.json(f"s3a://{BUCKET_NAME}/raw/social/type=location/")
        
        df_social_comments = df_social_raw.withColumn("comment_raw", explode(col("sample_comments"))) \
                                          .withColumn("comment", lower(col("comment_raw")))
        
        # Bộ lọc Spam (Regex)
        df_spam_filtered = df_social_comments.withColumn(
            "is_spam", 
            when(col("comment").rlike("giá rẻ|liên hệ zalo|tour trọn gói|book lịch|hotline|vé|tour"), 1).otherwise(0)
        )
        
        # Đập tan Spam: Xóa bài đánh dấu spam và xóa trùng lặp (De-duplication)
        df_clean_social = df_spam_filtered.filter(col("is_spam") == 0).dropDuplicates(["comment"])
        
        df_social_agg = df_clean_social.groupBy("entity_id") \
                                       .agg(_sum("buzz_volume").alias("clean_social_buzz"))
    except Exception as e:
        print(f"⚠️ Không tìm thấy dữ liệu FB/TikTok: {e}")
        df_social_agg = spark.createDataFrame([], schema="entity_id STRING, clean_social_buzz DOUBLE")

    # 4. JOIN & TÍNH TRUE-VIEW SCORE
    print("⚖️ Đang hợp nhất và tính toán True-View Score...")
    # FIX 2 (Tiếp nối): Join bằng loc_id_int đã được chuẩn hóa
    df_joined = df_locations.join(df_gmaps_agg, df_locations.id == df_gmaps_agg.loc_id_int, "left") \
                            .join(df_social_agg, df_locations.id == df_social_agg.entity_id, "left") \
                            .fillna(0)

    # Nơi nào Rating > 0 thì dùng hệ số Rating/5, nơi nào không có thì mặc định hệ số 0.5
    df_final = df_joined.withColumn(
        "true_view_score", 
        col("clean_social_buzz") * (when(col("avg_gmap_rating") > 0, col("avg_gmap_rating") / 5.0).otherwise(0.5))
    )

    # 5. LƯU KẾT QUẢ VÀO DATALAKE
    output_path = f"s3a://{BUCKET_NAME}/processed/travel_ranking/"
    print(f"💾 Đang lưu kết quả True-View Ranking vào: {output_path}")
    
    df_export = df_final.select("location_name", "clean_social_buzz", "avg_gmap_rating", "true_view_score")
    df_export.write.mode("overwrite").parquet(output_path)
    
    print("\n📊 BẢNG XẾP HẠNG TRAVEL (TRUE-VIEW V2.1):")
    df_export.orderBy(col("true_view_score").desc()).show(truncate=False)

    print("✅ HOÀN THÀNH TRAVEL PROCESSING!")
    spark.stop()

if __name__ == "__main__":
    main()