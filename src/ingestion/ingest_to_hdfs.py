import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, lit
from pyspark.sql.types import IntegerType

def get_spark_session(app_name):
    """
    Tạo Spark Session với cấu hình hỗ trợ Hive và MySQL JDBC
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def get_max_id_from_datalake(spark, hdfs_path):
    """
    Kiểm tra xem trong HDFS đã có dữ liệu chưa.
    Nếu có -> Trả về ID lớn nhất (Max ID) để làm mốc tải tiếp.
    Nếu chưa -> Trả về 0 (Tải từ đầu).
    """
    try:
        # Thử đọc dữ liệu từ HDFS
        # Chú ý: Spark sẽ báo lỗi nếu đường dẫn không tồn tại, nên cần try/except
        df_lake = spark.read.parquet(hdfs_path)
        
        # Nếu đọc được, tìm max id
        max_row = df_lake.agg(max("id")).collect()[0]
        max_id = max_row[0]
        
        if max_id is None:
            return 0
        return max_id
        
    except Exception as e:
        # Nếu lỗi (thường là do path chưa tồn tại), coi như chưa có dữ liệu
        print(f"⚠️ Chua co du lieu tai {hdfs_path}. Se tai tu dau (Full Load).")
        return 0

def main():
    # 1. Nhận tham số đầu vào (giống args của anh Cảnh)
    parser = argparse.ArgumentParser(description='Ingest MySQL to HDFS')
    parser.add_argument('--tblName', required=True, help='Ten bang trong MySQL')
    parser.add_argument('--executionDate', required=True, help='Ngay chay (YYYY-MM-DD)')
    args = parser.parse_args()

    table_name = args.tblName
    execution_date = args.executionDate
    
    # Tách ngày tháng năm để làm Partition
    y, m, d = execution_date.split('-')

    print(f"🚀 BAT DAU INGESTION: Table={table_name} | Date={execution_date}")

    # 2. Khởi tạo Spark
    spark = get_spark_session(f"Ingest {table_name}")

    # Cấu hình HDFS Path (Data Lake)
    hdfs_path = f"hdfs://namenode:9000/datalake/{table_name}"

    # 3. Logic Incremental Load (Kiểm tra dữ liệu cũ)
    # Lưu ý: Logic này áp dụng cho bảng có cột 'id' tăng dần (như travel, music, movie buzz)
    # Đối với bảng metadata (ít thay đổi), ta có thể chọn cách ghi đè (overwrite)
    
    # Ở đây demo logic Incremental cho bảng Buzz/Review
    max_id = 0
    # Chỉ áp dụng check max_id cho các bảng dữ liệu lớn, bảng metadata thì thôi
    if "buzz" in table_name or "data" in table_name: 
        # Tuy nhiên trong ngữ cảnh demo lần đầu, HDFS chưa có gì nên max_id luôn = 0
        # Để đơn giản cho lần chạy đầu, ta cứ set query lấy hết.
        pass 
    
    # Query lấy dữ liệu (Logic của anh Cảnh: SELECT * FROM table WHERE id > max_id)
    # Lưu ý: id trong file csv là UUID (chuỗi), không so sánh lớn nhỏ như số được.
    # Để demo đơn giản, ta sẽ load toàn bộ theo ngày (hoặc load hết nếu lần đầu).
    
    # Trong thực tế production với UUID, người ta thường dùng cột 'created_at' hoặc 'updated_at'
    # Ở đây mình sẽ load full bảng từ MySQL (vì dataset 10k dòng còn nhỏ với Spark)
    print("⏳ Dang doc du lieu tu MySQL...")
    
    jdbc_url = "jdbc:mysql://mysql:3306/social_trend_db?useSSL=false&allowPublicKeyRetrieval=true"
    
    df_source = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()

    print(f"📊 Da doc duoc {df_source.count()} dong tu MySQL.")

    # 4. Thêm cột Partition (Year, Month, Day)
    # Giả lập: Gán partition là ngày chạy job
    df_final = df_source \
        .withColumn("year", lit(y)) \
        .withColumn("month", lit(m)) \
        .withColumn("day", lit(d))

    # 5. Ghi xuống HDFS (Data Lake)
    print(f"💾 Dang ghi xuong HDFS tai: {hdfs_path} ...")
    
    df_final.write \
        .mode("append") \
        .partitionBy("year", "month", "day") \
        .parquet(hdfs_path)

    print("✅ INGESTION HOAN TAT!")
    spark.stop()

if __name__ == "__main__":
    main()