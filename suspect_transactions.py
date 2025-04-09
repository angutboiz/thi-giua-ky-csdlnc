from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder.appName("DetectAnomalies").getOrCreate()

student_id = "172100123"
df = spark.read.csv(f"transactions_{student_id}.csv", header=True, inferSchema=True)

df_cleaned = df.withColumn("price", col("price").cast("float")) \
               .withColumn("quantity", col("quantity").cast("int")) \
               .withColumn("discount", col("discount").cast("float")) \
               .withColumn("total_amount", col("quantity") * col("price") * (1 - col("discount")))

print("\n===== 🟣 PHÁT HIỆN GIAO DỊCH BẤT THƯỜNG - IQR 🟣 =====\n")

stats = df_cleaned.select(
    expr("percentile_approx(total_amount, 0.25)").alias("Q1"),
    expr("percentile_approx(total_amount, 0.5)").alias("Median"),
    expr("percentile_approx(total_amount, 0.75)").alias("Q3")
).collect()

Q1 = stats[0]["Q1"]
Q3 = stats[0]["Q3"]
Median = stats[0]["Median"]
IQR = Q3 - Q1
threshold = Median * 5  

suspect_transactions = df_cleaned.filter(col("total_amount") > threshold)
suspect_transactions.show()

output_file = f"suspect_transactions_{student_id}.csv"
suspect_transactions.write.csv(output_file, header=True, mode="overwrite")

print(f"\n✅ Phân tích hoàn thành! Kết quả đã lưu vào: {output_file}")