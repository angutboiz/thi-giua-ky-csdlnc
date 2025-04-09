from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

def normalize_product_name(product_name):
    """
    Normalize product names by:
    1. Removing special characters
    2. Converting to lowercase
    3. Stripping whitespace
    
    Args:
        product_name (str): The original product name
        
    Returns:
        str: The normalized product name
    """
    if product_name is None:
        return None
    
    # Remove special characters, keeping only alphanumeric and spaces
    normalized = re.sub(r'[^a-zA-Z0-9\s]', '', product_name)
    
    # Convert to lowercase
    normalized = normalized.lower()
    
    # Strip whitespace
    normalized = normalized.strip()
    
    return normalized

# Register the UDF
normalize_product_name_udf = udf(normalize_product_name, StringType())

def register_udfs(spark):
    """
    Register all UDFs with the Spark session
    
    Args:
        spark (SparkSession): The active Spark session
    """
    # Register the normalize_product_name function
    spark.udf.register("normalize_product_name", normalize_product_name, StringType())
    
    return {
        "normalize_product_name": normalize_product_name_udf
    }

if __name__ == "__main__":
    # Test the UDF
    spark = SparkSession.builder.appName("UDF Test").getOrCreate()
    
    # Sample data
    data = [
        ("Product-123!@#", ),
        ("  Fancy ITEM ", ),
        ("No$Special^Chars", )
    ]
    
    # Create a dataframe
    df = spark.createDataFrame(data, ["product_name"])
    
    # Register UDFs
    udfs = register_udfs(spark)
    
    # Apply the UDF
    df = df.withColumn("normalized_name", udfs["normalize_product_name"](df["product_name"]))
    
    # Show results
    print("=== Product Name Normalization Test ===")
    df.show(truncate=False)
    
    spark.stop()
