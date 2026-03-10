import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when, sum as _sum, count, current_timestamp, lit, current_date, explode
from pyspark.sql.types import StringType

# --- CẤU HÌNH ---
MINIO_ENDPOINT = "http://minio:9000"
BUCKET_NAME = "social-trend-lake"
DB_CONN = "jdbc:postgresql://postgres:5432/airflow"
DB_PROPERTIES = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

def get_spark_session():
    return SparkSession.builder \
        .appName("Movie Contextual Lifecycle Processing") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio_password_secure_123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()

def main():
    spark = get_spark_session()
    print("🚀 BẮT ĐẦU XỬ LÝ DỮ LIỆU MOVIE (CONTEXTUAL ENGINE)...")

    # 1. ĐỌC METADATA TỪ POSTGRES (Lấy các Topic có release_date -> Chính là Phim)
    df_topics = spark.read.jdbc(url=DB_CONN, table="topics", properties=DB_PROPERTIES)
    df_movies_meta = df_topics.filter(col("release_date").isNotNull()) \
                              .select("topic_id", "topic_name", "is_client", "release_date")

    # 2. ĐỌC DỮ LIỆU RAW SOCIAL TỪ MINIO
    try:
        df_raw = spark.read.json(f"s3a://{BUCKET_NAME}/raw/social/type=topic/*/*/*/*/*/*.json")
    except Exception as e:
        print(f"⚠️ Chưa có data Social: {e}")
        return

    # 3. JOIN DATA & CHUẨN BỊ NLP
    # Nối dữ liệu mxh với metadata phim
    df_joined = df_raw.join(df_movies_meta, df_raw.entity_id == df_movies_meta.topic_id, "inner")
    
    # BƯỚC SỬA LỖI: Tách việc bung mảng (explode) và hạ chữ thường (lower) ra làm 2 bước rõ ràng
    df_exploded = df_joined.withColumn("comment_raw", explode(col("sample_comments")))
    df_comments = df_exploded.withColumn("comment", lower(col("comment_raw")))

    # 4. BỘ LỌC NLP (PHÂN LOẠI Ý ĐỊNH - INTENT CLASSIFICATION)
    # Rổ A (Hype): hóng, mong chờ, trailer, poster
    # Rổ B (Review): kết phim, diễn xuất, khóc, xem
    df_intent = df_comments.withColumn("is_hype", when(col("comment").rlike("hóng|mong chờ|trailer|poster"), 1).otherwise(0)) \
                           .withColumn("is_review", when(col("comment").rlike("kết phim|diễn xuất|khóc|xem"), 1).otherwise(0))

    # Tổng hợp lại theo từng phim
    df_agg = df_intent.groupBy("topic_id", "topic_name", "is_client", "release_date") \
                      .agg(
                          _sum("buzz_volume").alias("total_buzz"),
                          _sum("is_hype").alias("hype_count"),
                          _sum("is_review").alias("review_count")
                      )

    # 5. GẮN TAG BẢO VỆ USER (LA BÀN NGỮ CẢNH)
    df_tagged = df_agg.withColumn(
        "context_tag",
        when((current_date() < col("release_date")) & (col("hype_count") > col("review_count")), 
             lit("[⚠️ ĐANG HYPE TRAILER - Ngắn Hạn]"))
        .when((current_date() >= col("release_date")) & (col("review_count") > col("hype_count")), 
             lit("[🔥 ĐANG CÔNG CHIẾU - Thời Điểm Vàng]"))
        .otherwise(lit("[❄️ COOLING DOWN - Đang hạ nhiệt]"))
    )

    # 6. LƯU KẾT QUẢ VÀO MINIO (Lớp Processed/Gold)
    # Ở V2.0 ta không dùng Hive Metastore nữa để tránh lỗi cũ, ta lưu thẳng file Parquet vào Datalake 
    output_path = f"s3a://{BUCKET_NAME}/processed/movie_ranking/"
    print(f"💾 Đang lưu kết quả phân tích Movie vào: {output_path}")
    df_tagged.write.mode("overwrite").parquet(output_path)
    
    # In ra console để Khang xem luôn
    print("📊 KẾT QUẢ RANKING MOVIE MỚI NHẤT:")
    df_tagged.select("topic_name", "total_buzz", "context_tag").show(truncate=False)

    print("✅ HOÀN THÀNH MOVIE PROCESSING!")
    spark.stop()

if __name__ == "__main__":
    main()