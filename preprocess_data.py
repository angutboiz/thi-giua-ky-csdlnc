from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_date, to_timestamp, weekofyear, countDistinct, sum, count, lag
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType, IntegerType, DateType, TimestampType
import sys
# Import the UDF module
from UDF import register_udfs

def preprocess_and_analyze(student_id):
    spark = SparkSession.builder.appName("TransactionAnalysis").getOrCreate()
    
    # Register UDFs
    udfs = register_udfs(spark)
    
    input_file = f"transactions_{student_id}.csv"
    cleaned_file = f"cleaned_data_{student_id}.csv"
    bad_rows_file = f"bad_rows_{student_id}.csv"
    weekly_spending_file = f"weekly_spending_{student_id}.csv"
    customer_behavior_file = f"customer_behavior_{student_id}.csv"
    declining_orders_file = f"declining_orders_{student_id}.csv"
    
    df = spark.read.option("header", "true").csv(input_file)
    
    # Xử lý lỗi dữ liệu
    def safe_cast(column, to_type):
        return expr(f"CASE WHEN {column} RLIKE '^[0-9]+(\\.[0-9]*)?$' THEN {column} ELSE NULL END").cast(to_type)
    
    df = df.withColumn("price", safe_cast("price", FloatType()))
    df = df.withColumn("quantity", safe_cast("quantity", IntegerType()))
    df = df.withColumn("discount", safe_cast("discount", FloatType()))
    
    # Fix: Use to_timestamp instead of to_date to handle timestamp values correctly
    # Then extract date if needed using to_date on the timestamp column
    df = df.withColumn("order_timestamp", to_timestamp(col("order_date")))
    df = df.withColumn("order_date", to_date(col("order_timestamp")))
    
    # Lưu dữ liệu bị lỗi
    bad_rows = df.filter("price IS NULL OR quantity IS NULL OR discount IS NULL OR order_date IS NULL")
    bad_rows.write.csv(bad_rows_file, header=True, mode="overwrite")
    
    # Lọc dữ liệu hợp lệ
    df_cleaned = df.filter("price IS NOT NULL AND quantity IS NOT NULL AND discount IS NOT NULL AND order_date IS NOT NULL")
    
    # Tính total_amount
    df_cleaned = df_cleaned.withColumn("total_amount", col("quantity") * col("price") * (1 - col("discount")))
    
    # Apply the normalization UDF to product_name column (assuming it exists)
    # If the column has a different name, adjust accordingly
    if "product_name" in df.columns:
        df_cleaned = df_cleaned.withColumn("normalized_product_name", 
                                           udfs["normalize_product_name"](col("product_name")))
    
    # Lưu dữ liệu đã xử lý
    df_cleaned.write.csv(cleaned_file, header=True, mode="overwrite")
    
   
    print(f"✅ Dữ liệu đã xử lý lưu tại: {cleaned_file}")
    print(f"✅ Dữ liệu lỗi lưu tại: {bad_rows_file}")

    spark.stop()

if __name__ == "__main__":
    preprocess_and_analyze(172100123)