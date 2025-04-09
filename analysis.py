from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum, to_date, weekofyear, count, month, lag
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("TransactionAnalysis").getOrCreate()

student_id = "172100123"
file_path = f"transactions_{student_id}.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

df = df.withColumn("order_date", to_date(col("order_date"))) \
       .withColumn("price", col("price").cast("float")) \
       .withColumn("quantity", col("quantity").cast("int")) \
       .withColumn("discount", col("discount").cast("float"))

df_cleaned = df.dropna()
df_bad_rows = df.filter(col("quantity").isNull() | col("price").isNull())
df_bad_rows.write.csv(f"bad_rows_{student_id}.csv", header=True, mode="overwrite")
df_cleaned = df_cleaned.withColumn("total_amount", col("quantity") * col("price") * (1 - col("discount")))

print("\n===== 🟢 TỔNG CHI TIÊU MỖI TUẦN THEO CUSTOMER_ID 🟢 =====\n")
weekly_spending = df_cleaned.withColumn("week", weekofyear(col("order_date"))) \
                            .groupBy("customer_id", "week") \
                            .agg(sum("total_amount").alias("weekly_spending"))

weekly_spending.show(10, False)
weekly_spending.write.csv(f"weekly_spending_{student_id}.csv", header=True, mode="overwrite")

print("\n===== 🔵 PHÂN CỤM HÀNH VI MUA HÀNG 🔵 =====\n")
customer_behavior = df_cleaned.groupBy("customer_id").agg(
    countDistinct("transaction_id").alias("total_orders"),
    sum("total_amount").alias("total_spent"),
    countDistinct("price").alias("unique_products")
)

print("📌 Thống kê hành vi mua hàng của từng khách hàng:")
customer_behavior.show(10, False)
customer_behavior.write.csv(f"customer_behavior_{student_id}.csv", header=True, mode="overwrite")

print("\n===== 🔴 KHÁCH HÀNG GIẢM ĐƠN HÀNG 3 THÁNG LIÊN TIẾP 🔴 =====\n")

df_monthly = df_cleaned.withColumn("month", month(col("order_date"))) \
                        .groupBy("customer_id", "month") \
                        .agg(count("transaction_id").alias("order_count"))

window_spec = Window.partitionBy("customer_id").orderBy("month")
df_monthly = df_monthly.withColumn("prev_order_count", lag("order_count").over(window_spec)) \
                       .withColumn("prev_prev_order_count", lag("order_count", 2).over(window_spec))

df_decreasing = df_monthly.filter(
    (col("prev_order_count").isNotNull()) &
    (col("prev_prev_order_count").isNotNull()) &
    (col("order_count") < col("prev_order_count")) &
    (col("prev_order_count") < col("prev_prev_order_count"))
).select("customer_id").distinct()

print("📌 Danh sách khách hàng có số đơn hàng giảm liên tiếp trong 3 tháng gần nhất:")
df_decreasing.show(10, False)
df_decreasing.write.csv(f"decreasing_orders_{student_id}.csv", header=True, mode="overwrite")