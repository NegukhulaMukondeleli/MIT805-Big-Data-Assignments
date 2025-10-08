# dataset_statistics.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
import humanize

def generate_dataset_statistics():
    """Generate comprehensive dataset statistics for the combined taxi data"""
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("DatasetStatistics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Read the combined data
    input_path = "hdfs://localhost:9870/MIT805A1/combined_all_taxi_data/nyc_all_taxi_data_2023_2024_combined.csv"
    
    print("Reading combined data...")
    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True
    )
    
    # Calculate statistics
    print("Calculating statistics...")
    
    # Basic statistics
    num_variables = len(df.columns)
    num_observations = df.count()
    
    # Missing values calculation
    missing_expr = [count(when(col(c).isNull() | (col(c) == ""), c)).alias(c) for c in df.columns]
    missing_counts = df.select(missing_expr).collect()[0]
    missing_cells = sum([missing_counts[c] for c in df.columns])
    missing_percentage = (missing_cells / (num_variables * num_observations)) * 100
    
    # Duplicate rows
    duplicate_rows = df.count() - df.distinct().count()
    duplicate_percentage = (duplicate_rows / num_observations) * 100 if num_observations > 0 else 0
    
    # Memory usage estimation (approximate)
    # We'll sample the data to estimate memory usage
    sample_fraction = 0.01  # 1% sample
    sample_df = df.sample(fraction=sample_fraction, seed=42)
    
    # Cache the sample to force computation and get memory info
    sample_df.cache()
    sample_df.count()  # Force caching
    
    # Get storage info
    storage_info = spark.sparkContext._jsc.sc().getRDDStorageInfo()
    sample_size = 0
    for info in storage_info:
        if info.name().contains(sample_df.rdd.name()):
            sample_size = info.memUsed()
            break
    
    # Calculate estimated total memory
    estimated_total_memory = (sample_size / sample_fraction) if sample_size > 0 else 0
    avg_record_size = estimated_total_memory / num_observations if num_observations > 0 else 0
    
    # Format memory sizes for readability
    total_memory_readable = humanize.naturalsize(estimated_total_memory, binary=True)
    avg_record_size_readable = humanize.naturalsize(avg_record_size, binary=True)
    
    # Create statistics table as a DataFrame
    stats_data = [
        ("Number of variables", num_variables, ""),
        ("Number of observations", num_observations, ""),
        ("Missing cells", missing_cells, f"{missing_percentage:.1f}%"),
        ("Duplicate rows", duplicate_rows, f"{duplicate_percentage:.1f}%"),
        ("Total size in memory", "", total_memory_readable),
        ("Average record size in memory", "", avg_record_size_readable)
    ]
    
    stats_schema = StructType([
        StructField("Metric", StringType(), True),
        StructField("Value", LongType(), True),
        StructField("Percentage", StringType(), True)
    ])
    
    stats_df = spark.createDataFrame(stats_data, stats_schema)
    
    # Show the statistics table
    print("\n" + "="*60)
    print("DATASET STATISTICS")
    print("="*60)
    
    stats_df.show(truncate=False)
    
    # Also print in a formatted way
    print("\nFormatted Statistics:")
    print("-" * 50)
    for row in stats_df.collect():
        if row["Percentage"]:
            print(f"{row['Metric']:35} {row['Value']} {row['Percentage']}")
        else:
            print(f"{row['Metric']:35} {row['Value']}")
    
    # Save the statistics to a CSV file
    output_path = "hdfs://localhost:9870/MIT805A1/dataset_statistics.csv"
    stats_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(output_path)
    
    print(f"\nStatistics saved to: {output_path}")
    
    spark.stop()

if __name__ == "__main__":
    generate_dataset_statistics()