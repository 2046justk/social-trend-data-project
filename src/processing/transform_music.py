from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, rand, round

def get_spark_session():
    return SparkSession.builder \
        .appName("Music Fair-Rank Transformation") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()

def main():
    spark = get_spark_session()
    
    # 1. Đọc dữ liệu thô từ Data Lake (HDFS)
    # Đọc tất cả các partition (để tổng hợp Ranking toàn cục)
    df_raw = spark.read.parquet("hdfs://namenode:9000/datalake/social_buzz_music")

    # 2. Logic tính điểm (Scoring Algorithm)
    # Giả lập: Sentiment score random từ 0.5 đến 1.0 (vì chưa có model NLP thật)
    # Organic Score = Interaction * Sentiment
    df_scored = df_raw.withColumn("sentiment_score", round(rand() * 0.5 + 0.5, 2)) \
                      .withColumn("organic_score", col("interaction_count") * col("sentiment_score"))

    # 3. Logic phân loại & "Boost" điểm cho Client
    # Nếu là Client (is_client=true) -> Boosted Score = Organic * 1.5 (Hệ số ưu tiên)
    # Nếu không phải -> Boosted Score = Organic
    df_final = df_scored.withColumn("boosted_score", 
                                    when(col("is_client") == True, col("organic_score") * 1.5)
                                    .otherwise(col("organic_score"))) \
                        .withColumn("status", lit("Active")) # Mặc định Active, logic inactive làm sau

    # 4. Lưu vào Data Warehouse (Hive)
    # Lưu ý: Overwrite để cập nhật Bảng Xếp Hạng mới nhất
    print("💾 Dang ghi ket qua vao Hive Table: music_fair_rankings ...")
    df_final.write.mode("overwrite").saveAsTable("social_trend_db.music_fair_rankings")
    
    print("✅ MUSIC TRANSFORMATION DONE!")
    spark.stop()

if __name__ == "__main__":
    main()