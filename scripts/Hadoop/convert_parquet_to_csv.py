from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ParquetToCSV") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Define paths
input_path = "hdfs:///MIT805A1/yellow_tripdata/*.parquet"
output_path = "hdfs:///MIT805A1/yellow_tripdata_combined_csv"
temp_output_path = "hdfs:///MIT805A1/yellow_tripdata_csv_temp"

try:
    # Read all parquet files
    print("Reading Parquet files...")
    df = spark.read.parquet(input_path)
    
    # Show schema and count
    print("Schema:")
    df.printSchema()
    print(f"Total records: {df.count()}")
    
    # Add source file name for tracking
    # df = df.withColumn("source_file", input_file_name())
    
    # Write as single CSV file with header
    print("Writing combined CSV file...")
    df.coalesce(1) \
      .write \
      .mode("overwrite") \
      .option("header", "true") \
      .option("delimiter", ",") \
      .csv(temp_output_path)
    
    # Rename the part file to a proper name
    print("Renaming output file...")
    
except Exception as e:
    print(f"Error: {e}")
    raise

finally:
    spark.stop()