import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum, lit
from pyspark.sql.types import StringType

# --- CẤU HÌNH ---
MINIO_ENDPOINT = "http://minio:9000"
BUCKET_NAME = "social-trend-lake"
DB_CONN = "jdbc:postgresql://postgres:5432/airflow"
DB_PROPERTIES = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

def get_spark_session():
    return SparkSession.builder \
        .appName("Music Lifecycle Fairness Engine V2.1") \
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
    print("🚀 BẮT ĐẦU XỬ LÝ DỮ LIỆU MUSIC (FAIRNESS ENGINE)...")

    # 1. ĐỌC METADATA
    df_topics = spark.read.jdbc(url=DB_CONN, table="topics", properties=DB_PROPERTIES)
    df_music_meta = df_topics.filter(col("release_date").isNull()) \
                             .select(col("topic_id").cast("string").alias("meta_topic_id"), "topic_name", "is_client")

    # 2. ĐỌC DỮ LIỆU YOUTUBE (Cấu trúc đã sạch rác, trỏ thẳng thư mục gốc)
    print("🎵 Đang tổng hợp dữ liệu YouTube...")
    try:
        # Bỏ dấu * rườm rà. Trỏ thẳng vào thư mục chứa các topic_id
        df_yt = spark.read.option("multiline", "true") \
                          .json(f"s3a://{BUCKET_NAME}/raw/music/")
        
        df_yt_agg = df_yt.groupBy("topic_id").agg(
            (_sum("view_count") + _sum("like_count") + _sum("comment_count")).alias("yt_total_buzz")
        )
    except Exception as e:
        print(f"⚠️ Không tìm thấy dữ liệu YouTube: {e}")
        df_yt_agg = spark.createDataFrame([], schema="topic_id STRING, yt_total_buzz DOUBLE")

    # 3. ĐỌC DỮ LIỆU FB/TIKTOK (Fix lỗi Multiline)
    print("📱 Đang tổng hợp dữ liệu FB/TikTok...")
    try:
        df_social = spark.read.option("multiline", "true").json(f"s3a://{BUCKET_NAME}/raw/social/type=topic/")
        df_social_agg = df_social.groupBy("entity_id").agg(_sum("buzz_volume").alias("social_buzz"))
    except Exception as e:
        print(f"⚠️ Không tìm thấy dữ liệu FB/TikTok: {e}")
        df_social_agg = spark.createDataFrame([], schema="entity_id STRING, social_buzz DOUBLE")

    # 4. HỢP NHẤT VÀ TÍNH TỔNG BUZZ THẬT
    print("⚖️ Đang chạy Rule Sinh Tử (Lifecycle Rules)...")
    df_joined = df_music_meta.join(df_yt_agg, df_music_meta.meta_topic_id == df_yt_agg.topic_id, "left") \
                             .join(df_social_agg, df_music_meta.meta_topic_id == df_social_agg.entity_id, "left") \
                             .fillna(0)
    
    df_total = df_joined.withColumn("total_buzz", col("yt_total_buzz") + col("social_buzz"))

    # 5. GẮN TAG CÔNG BẰNG (FAIRNESS RULES)
    HOT_THRESHOLD = 2000
    df_fairness = df_total.withColumn(
        "lifecycle_tag",
        when((col("is_client") == True) & (col("total_buzz") < HOT_THRESHOLD), lit("[👻 ZOMBIE - Client Ẩn]"))
        .when((col("is_client") == True) & (col("total_buzz") >= HOT_THRESHOLD), lit("[🌟 CLIENT - Đang Hot]"))
        .when((col("is_client") == False) & (col("total_buzz") >= HOT_THRESHOLD), lit("[⚡ AUTO-EXTENDED - Organic Hot]"))
        .otherwise(lit("[💀 DEAD - Organic Chết Yểu]"))
    )

    # 6. LƯU KẾT QUẢ VÀO DATALAKE
    output_path = f"s3a://{BUCKET_NAME}/processed/music_ranking/"
    print(f"💾 Đang lưu kết quả Music Fairness vào: {output_path}")
    
    df_export = df_fairness.select("topic_name", "is_client", "total_buzz", "lifecycle_tag")
    df_export.write.mode("overwrite").parquet(output_path)
    
    print("\n📊 BẢNG XẾP HẠNG MUSIC (LIFECYCLE FAIRNESS V2.1):")
    df_export.orderBy(col("total_buzz").desc()).show(truncate=False)

    print("✅ HOÀN THÀNH MUSIC PROCESSING!")
    spark.stop()

if __name__ == "__main__":
    main()