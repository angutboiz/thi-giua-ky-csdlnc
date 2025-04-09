from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import matplotlib.pyplot as plt
import seaborn as sns

# 🔥 1. Khởi tạo SparkSession
spark = SparkSession.builder.appName("CustomerClustering").getOrCreate()

# 🟢 2. Đọc dữ liệu CSV
student_id = "172100123"
df = spark.read.csv(f"transactions_{student_id}.csv", header=True, inferSchema=True)

# 🟣 3. Phân cụm khách hàng bằng KMeans (Spark ML)
# ✅ Tính tổng số đơn hàng & tổng chi tiêu theo từng khách hàng
customer_data = df.groupBy("customer_id").agg(
    {"transaction_id": "count", "price": "sum"}
).withColumnRenamed("count(transaction_id)", "total_orders") \
 .withColumnRenamed("sum(price)", "total_spent")

# ✅ Chuyển đổi dữ liệu thành dạng vector cho KMeans
assembler = VectorAssembler(inputCols=["total_orders", "total_spent"], outputCol="features")
customer_features = assembler.transform(customer_data)

# ✅ Áp dụng KMeans với K=3
kmeans = KMeans(k=3, seed=1, featuresCol="features", predictionCol="cluster")
model = kmeans.fit(customer_features)
clustered_customers = model.transform(customer_features)

# 🎨 4. Trực quan hóa bằng Seaborn
import pandas as pd

# ✅ Chuyển dữ liệu sang Pandas để vẽ biểu đồ
clustered_pd = clustered_customers.select("customer_id", "total_orders", "total_spent", "cluster").toPandas()

# ✅ Vẽ scatter plot
plt.figure(figsize=(10, 6))
sns.scatterplot(x="total_orders", y="total_spent", hue="cluster", data=clustered_pd, palette="viridis", s=100)
plt.xlabel("Tổng số đơn hàng")
plt.ylabel("Tổng chi tiêu")
plt.title("Phân cụm khách hàng bằng KMeans")
plt.legend(title="Cụm")
plt.grid()
plt.show()

print("\n✅ Phân tích hoàn thành!")