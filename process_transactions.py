from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, TimestampType, StringType
from pyspark.sql.functions import col, expr, udf, regexp_replace, lower, trim
import os
import shutil
import glob
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

def rename_part_csv(output_dir, new_name):
    # Tìm file part-*.csv
    part_files = glob.glob(os.path.join(output_dir, "part-*.csv"))
    if part_files:
        src = part_files[0]
        dst = os.path.join("output", new_name)  # Ghi ra cùng cấp với file .py
        shutil.move(src, dst)
        print(f"✅ Đã ghi file: {dst}")
    else:
        print(f"❌ Không tìm thấy file part trong {output_dir}")
    
def analytics_data(df_valid, spark):
    '''Tổng chi tiêu mỗi tuần theo từng customer_id'''
    from pyspark.sql.functions import weekofyear, year, sum as _sum
    from pyspark.sql.functions import count, countDistinct
    
    weekly_spending = df_valid.withColumn("year", year("order_date")) \
                            .withColumn("week", weekofyear("order_date")) \
                            .groupBy("customer_id", "year", "week") \
                            .agg(_sum("total_amount").alias("total_weekly_spending")) \
                            .orderBy("customer_id", "year", "week")

    weekly_spending.show()
    
    ''' Phân cụm hành vi mua hàng của mỗi khách hàng'''
    customer_behavior = df_valid.withColumn("real_spending", col("price") * col("quantity") * (1 - col("discount"))) \
    .groupBy("customer_id").agg(
    countDistinct("transaction_id").alias("total_orders"),
    _sum("real_spending").alias("total_spent"),
    countDistinct("location").alias("unique_locations")  # Hoặc nếu bạn có cột product thì đổi lại
)

    customer_behavior.show()
    
    
    ''' Truy vấn Spark SQL: "Khách hàng nào có số đơn hàng giảm liên tiếp trong 3 tháng gần nhất?" '''
    
    df_valid.createOrReplaceTempView("transactions")

    query = """
    WITH monthly_orders AS (
        SELECT customer_id,
            YEAR(CAST(order_date AS DATE)) AS year,
            MONTH(CAST(order_date AS DATE)) AS month,
            COUNT(DISTINCT transaction_id) AS order_count
        FROM transactions
        GROUP BY customer_id, YEAR(CAST(order_date AS DATE)), MONTH(CAST(order_date AS DATE))
    ),
    recent_orders AS (
        SELECT customer_id, year, month, order_count,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY year DESC, month DESC) as rn
        FROM monthly_orders
    ),
    last_3_months AS (
        SELECT * FROM recent_orders WHERE rn <= 3
    ),
    agg AS (
        SELECT customer_id,
            collect_list(order_count) as orders_last_3
        FROM last_3_months
        GROUP BY customer_id
    )
    SELECT customer_id
    FROM agg
    WHERE size(orders_last_3) = 3
    AND orders_last_3[0] > orders_last_3[1]
    AND orders_last_3[1] > orders_last_3[2]
    """

    result = spark.sql(query)
    result.coalesce(1).write.option("header", True).mode("overwrite").csv("output/decreasing_orders")
    result.show()

def detect_unusual_transactions(df, spark, student_id):
    """
    Phát hiện giao dịch bất thường dựa trên Z-score và IQR
    """
    from pyspark.sql.functions import mean, stddev, expr, percentile_approx
    
    # Tính thống kê để phát hiện bất thường
    stats = df.select(
        mean("total_amount").alias("mean"),
        stddev("total_amount").alias("stddev"),
        percentile_approx("total_amount", 0.5).alias("median"),
        percentile_approx("total_amount", 0.25).alias("q1"),
        percentile_approx("total_amount", 0.75).alias("q3")
    ).collect()[0]
    
    mean_val = stats["mean"]
    stddev_val = stats["stddev"]
    median_val = stats["median"]
    q1_val = stats["q1"]
    q3_val = stats["q3"]
    iqr_val = q3_val - q1_val
    
    # Z-score threshold (thường là 3)
    z_score_threshold = 3
    
    # IQR threshold (thường là 1.5)
    iqr_threshold = 1.5
    
    # Áp dụng cả hai phương pháp
    df_with_zscore = df.withColumn("z_score", expr(f"(total_amount - {mean_val})/{stddev_val}"))
    
    # Tìm các giao dịch bất thường theo Z-score
    unusual_by_zscore = df_with_zscore.filter(
        expr(f"abs(z_score) > {z_score_threshold}")
    ).drop("z_score")  # Drop the z_score column before union
    
    # Tìm các giao dịch bất thường theo IQR
    unusual_by_iqr = df.filter(
        (col("total_amount") < (q1_val - iqr_threshold * iqr_val)) |
        (col("total_amount") > (q3_val + iqr_threshold * iqr_val))
    )
    
    # Tìm các giao dịch có total_amount cao bất thường (> 5 lần trung vị)
    unusual_by_median = df.filter(col("total_amount") > 5 * median_val)
    
    # Kết hợp các kết quả (union loại bỏ trùng lặp)
    union_result = unusual_by_zscore.union(unusual_by_iqr).union(unusual_by_median).distinct()
    
    # Ghi kết quả vào file
    output_dir = f"output/suspect_transactions_{student_id}"
    union_result.coalesce(1).write.option("header", True).mode("overwrite").csv(output_dir)
    
    print(f"✅ Đã phát hiện {union_result.count()} giao dịch bất thường")
    return output_dir

def standardize_product_name_udf():
    """
    UDF để chuẩn hóa tên sản phẩm:
    1. Xóa ký tự đặc biệt
    2. Viết thường
    3. Strip khoảng trắng
    """
    def standardize(name):
        if name is None:
            return None
        # Xóa ký tự đặc biệt (giữ lại chữ, số và khoảng trắng)
        cleaned = ''.join(c for c in name if c.isalnum() or c.isspace())
        # Viết thường và strip khoảng trắng
        return cleaned.lower().strip()
    
    return udf(standardize, StringType())

def cluster_customers(df_valid, spark):
    """
    Dùng Spark ML để phân cụm khách hàng sử dụng KMeans và visualize kết quả
    """
    from pyspark.sql.functions import sum as _sum, count, avg, max as _max, min as _min, countDistinct
    
    try:
        # Simplified feature set to avoid potential dimension issues
        customer_features = df_valid.groupBy("customer_id").agg(
            _sum("total_amount").alias("total_spent"),
            avg("total_amount").alias("avg_order_value"),
            count("transaction_id").alias("transaction_count")
        )
        
        # Drop any rows with null values
        customer_features = customer_features.na.drop()
        
        # Cache to improve performance and check if we have enough data
        customer_features.cache()
        row_count = customer_features.count()
        
        print(f"Number of customers for clustering: {row_count}")
        
        if row_count < 2:
            return fallback_clustering(df_valid)
            
        # Log feature stats to debug
        print("Feature statistics:")
        customer_features.describe().show()
        
        # Make sure we have no zero values that might cause issues
        customer_features = customer_features.filter(
            (col("total_spent") > 0) & 
            (col("avg_order_value") > 0) & 
            (col("transaction_count") > 0)
        )
        
        # Standardize features - this is crucial for KMeans to work well
        assembler = VectorAssembler(
            inputCols=["total_spent", "avg_order_value", "transaction_count"],
            outputCol="features"
        )
        
        assembled_data = assembler.transform(customer_features)
        
        # Check for empty feature vectors
        empty_vectors = assembled_data.filter("size(features) = 0").count()
        if empty_vectors > 0:
            print(f"Warning: Found {empty_vectors} empty feature vectors, filtering them out")
            assembled_data = assembled_data.filter("size(features) > 0")
        
        # Use a more robust scaler
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )
        
        # Fit the scaler and transform the data
        scaler_model = scaler.fit(assembled_data)
        scaled_data = scaler_model.transform(assembled_data)
        
        # Try different k values, starting with a smaller range
        best_k = 2  # Default to 2 if all else fails
        best_score = -1
        
        # Only try a few k values to avoid potential issues
        for k in range(2, 4):
            try:
                print(f"Trying clustering with k={k}")
                
                # Set explicit initialization mode and increase max iterations
                kmeans = KMeans(
                    k=k, 
                    seed=42,
                    initMode="k-means||",
                    maxIter=20,
                    featuresCol="scaled_features"
                )
                
                model = kmeans.fit(scaled_data)
                predictions = model.transform(scaled_data)
                
                # Evaluate with silhouette score
                evaluator = ClusteringEvaluator(
                    predictionCol="prediction", 
                    featuresCol="scaled_features",
                    metricName="silhouette"
                )
                
                score = evaluator.evaluate(predictions)
                print(f"Silhouette score for k={k}: {score}")
                
                if score > best_score:
                    best_score = score
                    best_k = k
                    best_model = model
                    best_predictions = predictions
            
            except Exception as e:
                print(f"Error fitting KMeans with k={k}: {e}")
                continue
        
        print(f"Best number of clusters (k): {best_k}")
        
        # Convert to Pandas for visualization
        pandas_df = best_predictions.select(
            "customer_id", "total_spent", "avg_order_value", 
            "transaction_count", "prediction"
        ).toPandas()
        
        # Lưu visualizations
        save_cluster_visualizations(pandas_df, best_k)
        
        # Phân tích từng cụm
        cluster_analysis = best_predictions.groupBy("prediction").agg(
            count("customer_id").alias("customer_count"),
            avg("total_spent").alias("avg_total_spent"),
            avg("avg_order_value").alias("avg_order_value"),
            avg("transaction_count").alias("avg_transaction_count")
        ).orderBy("prediction")
        
        print("Cluster Analysis:")
        cluster_analysis.show()
        
        return pandas_df, best_k
    
    except Exception as e:
        print(f"Error in customer clustering: {e}")
        return fallback_clustering(df_valid)

def fallback_clustering(df_valid):
    """
    Fallback method for when ML clustering fails - do a simple manual segmentation
    """
    from pyspark.sql.functions import sum as _sum, count, avg, ntile, lit, when
    from pyspark.sql.window import Window
    
    print("Using fallback clustering method")
    
    # Create a simple RFM-like segmentation using percentiles
    # Fix: Explicitly name the average value column
    customer_features = df_valid.groupBy("customer_id").agg(
        _sum("total_amount").alias("total_spent"),
        count("transaction_id").alias("frequency")
    )
    
    # Calculate average separately with a proper alias that will be easier to reference
    customer_features = customer_features.withColumn(
        "average_value", 
        col("total_spent") / col("frequency")
    )
    
    # Add percentile rankings (manually segment into 2 groups - high/low)
    avg_spent = customer_features.agg(avg("total_spent")).collect()[0][0]
    avg_frequency = customer_features.agg(avg("frequency")).collect()[0][0]
    
    segmented = customer_features.withColumn(
        "monetary_segment", 
        when(col("total_spent") > avg_spent, 1).otherwise(0)
    ).withColumn(
        "frequency_segment", 
        when(col("frequency") > avg_frequency, 1).otherwise(0)
    )
    
    # Create final segment (0-3)
    segmented = segmented.withColumn(
        "prediction", 
        col("monetary_segment") + col("frequency_segment")
    )
    
    # Convert to pandas for visualization - Fix: Use the correct column name
    pandas_df = segmented.select(
        "customer_id", "total_spent", "average_value", "frequency", "prediction"
    ).toPandas()
    
    # Make a simple visualization
    save_cluster_visualizations(pandas_df, 2)
    
    # Show analysis
    segment_analysis = segmented.groupBy("prediction").agg(
        count("customer_id").alias("customer_count"),
        avg("total_spent").alias("avg_total_spent"),
        avg("frequency").alias("avg_frequency")
    ).orderBy("prediction")
    
    print("Manual Segmentation Analysis:")
    segment_analysis.show()
    
    return pandas_df, 2

def save_cluster_visualizations(pandas_df, k):
    """
    Tạo và lưu biểu đồ phân cụm
    """
    try:
        plt.figure(figsize=(12, 8))
        
        # Make sure columns exist
        x_col = 'total_spent'
        
        # Choose available y_column
        available_columns = ['avg_order_value', 'transaction_count', 'frequency', 'average_value']
        y_col = next((col for col in available_columns if col in pandas_df.columns), x_col)
        
        # Scatter plot
        scatter = plt.scatter(
            pandas_df[x_col], 
            pandas_df[y_col],
            c=pandas_df['prediction'], 
            cmap='viridis', 
            alpha=0.6,
            s=50,
            edgecolors='w'
        )
        
        plt.colorbar(scatter, label='Cluster')
        plt.xlabel(x_col.replace('_', ' ').title())
        plt.ylabel(y_col.replace('_', ' ').title())
        plt.title(f'Customer Clusters (k={k}): {x_col.replace("_", " ").title()} vs {y_col.replace("_", " ").title()}')
        plt.grid(True, alpha=0.3)
        
        # Improve appearance
        plt.tight_layout()
        
        # Save the figure
        plt.savefig('customer_clusters.png')
        print("✅ Lưu biểu đồ phân cụm tại: customer_clusters.png")
        
        # Second visualization if we have enough columns
        if ('transaction_count' in pandas_df.columns or 'frequency' in pandas_df.columns) and len(pandas_df) > 1:
            plt.figure(figsize=(12, 8))
            
            # Choose frequency column
            freq_col = 'transaction_count' if 'transaction_count' in pandas_df.columns else 'frequency'
            
            scatter2 = plt.scatter(
                pandas_df[freq_col], 
                pandas_df[x_col],
                c=pandas_df['prediction'], 
                cmap='viridis', 
                alpha=0.6,
                s=50,
                edgecolors='w'
            )
            
            plt.colorbar(scatter2, label='Cluster')
            plt.xlabel(freq_col.replace('_', ' ').title())
            plt.ylabel(x_col.replace('_', ' ').title())
            plt.title(f'Customer Clusters (k={k}): {freq_col.replace("_", " ").title()} vs {x_col.replace("_", " ").title()}')
            plt.grid(True, alpha=0.3)
            
            plt.tight_layout()
            plt.savefig('customer_clusters_transactions.png')
            print("✅ Lưu biểu đồ phân cụm thứ hai tại: customer_clusters_transactions.png")
    except Exception as e:
        print(f"Error creating visualizations: {e}")
        
        # Create a very simple fallback visualization
        try:
            plt.figure(figsize=(10, 6))
            labels = [f"Cluster {i}" for i in range(k)]
            sizes = pandas_df['prediction'].value_counts().sort_index().values
            plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
            plt.axis('equal')
            plt.title('Customer Distribution by Cluster')
            plt.savefig('customer_clusters_simple.png')
            print("✅ Lưu biểu đồ đơn giản tại: customer_clusters_simple.png")
        except:
            print("❌ Không thể tạo biểu đồ")

def apply_product_name_standardization(df):
    """
    Áp dụng UDF chuẩn hóa tên sản phẩm cho dataframe
    """
    # Kiểm tra xem có cột product_name không
    if "product_name" in df.columns:
        std_udf = standardize_product_name_udf()
        return df.withColumn("standardized_product_name", std_udf(col("product_name")))
    elif "product" in df.columns:
        std_udf = standardize_product_name_udf()
        return df.withColumn("standardized_product_name", std_udf(col("product")))
    else:
        # Tạo một cột giả lập để demo UDF
        print("⚠️ Không tìm thấy cột product_name hoặc product, tạo cột demo cho UDF")
        from pyspark.sql.functions import lit
        demo_df = df.withColumn("demo_product_name", 
                               lit("Demo PRoduct-123 !@#  with SPECIAL chars"))
        std_udf = standardize_product_name_udf()
        return demo_df.withColumn("standardized_product_name", 
                                std_udf(col("demo_product_name")))

def main():
    student_id = 172100123
    input_file = f"transactions_{student_id}.csv"

    # Tạo thư mục output nếu chưa có
    os.makedirs("output", exist_ok=True)

    clean_file = "output/clean_data"
    bad_file = f"output/bad_rows_{student_id}"

    spark = SparkSession.builder.appName("TransactionCleaner").getOrCreate()

    df_raw = spark.read.option("header", True).csv(input_file)

    df_casted = df_raw \
        .withColumn("order_date", col("order_date").cast(TimestampType())) \
        .withColumn("price", col("price").cast(FloatType())) \
        .withColumn("quantity", col("quantity").cast(IntegerType())) \
        .withColumn("discount", col("discount").cast(FloatType()))

    df_valid_base = df_casted.filter(
        col("transaction_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("order_date").isNotNull() &
        col("price").isNotNull() &
        col("quantity").isNotNull() &
        col("discount").isNotNull() &
        col("location").isNotNull()
    )

    df_valid = df_valid_base.withColumn("total_amount", expr("quantity * price * (1 - discount)"))

    df_bad = df_casted.subtract(df_valid_base)

    # Ghi file
    df_valid.coalesce(1).write.option("header", True).mode("overwrite").csv(clean_file)
    df_bad.coalesce(1).write.option("header", True).mode("overwrite").csv(bad_file)

    print(f"✅ Dữ liệu sạch lưu tại: {clean_file}")
    print(f"⚠️ Dữ liệu lỗi lưu tại: {bad_file}")
    
    # Run analytics before stopping Spark
    analytics_data(df_valid, spark)
    
    # Detect unusual transactions
    suspect_output_dir = detect_unusual_transactions(df_valid, spark, student_id)
    
    # Bonus 1: Standardize product names using UDF
    print("\n=== Bonus 1: Standardize Product Names ===")
    df_with_std_names = apply_product_name_standardization(df_valid)
    print("UDF Demo - Standardized Product Names:")
    df_with_std_names.select("demo_product_name", "standardized_product_name").distinct().show(5)
    
    # Bonus 2: Cluster customers using KMeans
    print("\n=== Bonus 2: Customer Clustering ===")
    try:
        cluster_df, best_k = cluster_customers(df_valid, spark)
        print(f"✅ Khách hàng được phân thành {best_k} cụm")
    except Exception as e:
        print(f"❌ Lỗi khi phân cụm khách hàng: {e}")
    
    # Rename the files
    rename_part_csv("output/clean_data", "clean_data.csv")
    rename_part_csv(f"output/bad_rows_{student_id}", "bad_rows.csv")
    rename_part_csv("output/decreasing_orders", "decreasing_orders.csv")
    rename_part_csv(suspect_output_dir, f"suspect_transactions_{student_id}.csv")
    
    # Stop the Spark session after all operations are complete
    spark.stop()

if __name__ == "__main__":
    main()
