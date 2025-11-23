from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, current_date, when, lit, to_date

def get_spark_session():
    return SparkSession.builder \
        .appName("Movie Cine-Pulse Transformation") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()

def main():
    spark = get_spark_session()
    
    # 1. Đọc 2 nguồn dữ liệu từ Data Lake
    # Lưu ý: Phải drop ngay các cột partition cũ đi để tránh trùng lặp/xung đột về sau
    df_buzz = spark.read.parquet("hdfs://namenode:9000/datalake/social_buzz_movie") \
                   .drop("year", "month", "day") 
    
    df_meta = spark.read.parquet("hdfs://namenode:9000/datalake/movies_metadata") \
                   .drop("year", "month", "day")

    # 2. JOIN dữ liệu
    # Join theo tên phim (movie_name)
    # VÀ QUAN TRỌNG: Drop các cột trùng lặp sau khi join
    df_joined = df_buzz.join(df_meta, df_buzz.movie_name == df_meta.movie_name, "inner") \
                       .drop(df_meta.movie_name) \
                       .drop(df_meta.movie_id)  # <-- THÊM DÒNG NÀY: Bỏ movie_id của bảng bên phải (meta) đi

    # 3. Logic Vòng Đời (Lifecycle Logic)
    # Tính khoảng cách ngày: Ngày thảo luận - Ngày công chiếu
    # Lưu ý: timestamp trong buzz là string/timestamp, cần đảm bảo format
    df_calced = df_joined.withColumn("days_diff", 
                                     datediff(to_date(col("timestamp")), to_date(col("release_date"))))

    # 4. Gắn nhãn tự động (Auto-Tagging)
    # < -30: Early Hype (Tin đồn)
    # -7 đến +7: Prime Time (Công chiếu - Quan trọng nhất)
    # > 30: Cooling Down (Hết hot)
    df_final = df_calced.withColumn("lifecycle_tag",
        when(col("days_diff") < -30, "Early Hype")
        .when((col("days_diff") >= -7) & (col("days_diff") <= 7), "Prime Time")
        .when(col("days_diff") > 30, "Cooling Down")
        .otherwise("Normal Run")
    )

    # 5. Lưu vào Data Warehouse
    print("💾 Dang ghi ket qua vao Hive Table: movie_cine_pulse ...")
    df_final.write.mode("overwrite").saveAsTable("social_trend_db.movie_cine_pulse")
    
    print("✅ MOVIE TRANSFORMATION DONE!")
    spark.stop()

if __name__ == "__main__":
    main()