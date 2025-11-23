from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, lit

def get_spark_session():
    return SparkSession.builder \
        .appName("Travel True-View Transformation") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()

def main():
    spark = get_spark_session()
    
    # 1. Đọc dữ liệu thô
    df_raw = spark.read.parquet("hdfs://namenode:9000/datalake/social_buzz_travel")

    # 2. Định nghĩa từ khóa Spam (Commercial Keywords)
    spam_keywords = "liên hệ|giá vé|tour trọn gói|xe đưa đón|inbox giá|zalo"
    
    # 3. Logic Phân loại (Classification)
    # Dùng hàm rlike (Regex Like) để quét nội dung
    df_classified = df_raw.withColumn("is_commercial", 
                                      when(lower(col("content")).rlike(spam_keywords), True)
                                      .otherwise(False))

    # 4. Tính True Score (Weighted Scoring)
    # Nếu là Commercial -> Trọng số 0.1 (Giảm 90% giá trị)
    # Nếu là Review thật -> Trọng số 1.0
    df_final = df_classified.withColumn("weight", 
                                        when(col("is_commercial") == True, 0.1).otherwise(1.0)) \
                            .withColumn("true_score", 10 * col("weight")) # Giả lập score cơ bản là 10

    # 5. Lưu vào Data Warehouse
    print("💾 Dang ghi ket qua vao Hive Table: travel_true_reviews ...")
    df_final.write.mode("overwrite").saveAsTable("social_trend_db.travel_true_reviews")
    
    print("✅ TRAVEL TRANSFORMATION DONE!")
    spark.stop()

if __name__ == "__main__":
    main()