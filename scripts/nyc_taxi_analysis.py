# nyc_taxi_analysis.py
import pandas as pd
import subprocess
import tempfile
import os
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

def run_command(command):
    """Run a command and return success status and output"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        return result.returncode == 0, result.stdout
    except Exception as e:
        return False, str(e)

def download_sample_file(hdfs_path, temp_dir, sample_size=5000):
    """Download a sample file from HDFS and return DataFrame"""
    print(f"Downloading sample data from: {hdfs_path}")
    
    local_path = os.path.join(temp_dir, "temp_sample.csv")
    full_download_path = os.path.join(temp_dir, "full_download.csv")
    
    # First, check if the file exists and get its size
    exists, _ = run_command(f"hdfs dfs -test -e {hdfs_path}")
    if not exists:
        print(f"❌ File does not exist: {hdfs_path}")
        return None
    
    success, output = run_command(f"hdfs dfs -du -h {hdfs_path}")
    if success:
        print(f"File size: {output.split()[0]}")
    
    # Method 1: Download the complete file first (this will work on Windows)
    print("Downloading complete file (this may take a while)...")
    success, output = run_command(f"hdfs dfs -get {hdfs_path} {full_download_path}")
    
    if not success:
        print(f"❌ Download failed: {output}")
        return None
    
    print(f"Downloaded complete file: {os.path.getsize(full_download_path)} bytes")
    
    # Method 2: Create a sample from the downloaded file
    print("Creating sample from downloaded file...")
    try:
        # Count total lines first
        line_count = 0
        with open(full_download_path, 'r', encoding='utf-8') as f:
            for line in f:
                line_count += 1
        
        print(f"Total lines in file: {line_count}")
        
        # Determine how many lines to sample (header + sample_size)
        sample_lines = min(sample_size + 1, line_count)
        
        # Read the sample
        with open(full_download_path, 'r', encoding='utf-8') as f:
            with open(local_path, 'w', encoding='utf-8') as out_f:
                for i, line in enumerate(f):
                    if i >= sample_lines:
                        break
                    out_f.write(line)
        
        print(f"Created sample with {sample_lines} lines")
        
    except Exception as e:
        print(f"❌ Error creating sample: {e}")
        return None
    
    # Read the sample CSV file
    print("Reading sample data...")
    try:
        # First, let's check what's in the file
        with open(local_path, 'r', encoding='utf-8') as f:
            first_few_lines = []
            for i in range(3):
                line = f.readline()
                if not line:
                    break
                first_few_lines.append(line)
        
        print("First few lines of downloaded file:")
        for i, line in enumerate(first_few_lines):
            print(f"  {i+1}: {line.strip()}")
        
        # Now try to read with pandas
        df = pd.read_csv(local_path)
        print(f"✅ Successfully processed {len(df):,} rows from sample data")
        print(f"Columns found: {list(df.columns)}")
        return df
        
    except Exception as e:
        print(f"❌ Error processing sample data: {e}")
        # Let's see what's actually in the file
        try:
            with open(local_path, 'r', encoding='utf-8') as f:
                content = f.read(500)  # Read first 500 characters
            print(f"File content preview:\n{content}")
        except Exception as read_error:
            print(f"Could not read file content: {read_error}")
        return None

def analyze_data(df):
    """Perform comprehensive data analysis and generate visualizations"""
    print("\n" + "="*50)
    print("DATA ANALYSIS REPORT")
    print("="*50)
    
    # Basic information
    print(f"Dataset Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Display basic statistics
    print("\n1. BASIC STATISTICS:")
    print(df.describe())
    
    # Check for missing values
    print("\n2. MISSING VALUES:")
    missing = df.isnull().sum()
    missing_percent = (missing / len(df)) * 100
    missing_df = pd.DataFrame({
        'Missing Values': missing,
        'Percentage': missing_percent
    })
    print(missing_df[missing_df['Missing Values'] > 0])
    
    # Data quality checks
    print("\n3. DATA QUALITY CHECKS:")
    
    # Check for negative values in columns that shouldn't have them
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if col in df.columns and df[col].min() < 0:
            negative_count = (df[col] < 0).sum()
            print(f"  - {col}: {negative_count} negative values ({negative_count/len(df)*100:.2f}%)")
    
    # Check for zero passenger counts
    if 'passenger_count' in df.columns:
        zero_passengers = (df['passenger_count'] == 0).sum()
        print(f"  - Zero passenger trips: {zero_passengers} ({zero_passengers/len(df)*100:.2f}%)")
    
    # Check for zero distance trips
    if 'trip_distance' in df.columns:
        zero_distance = (df['trip_distance'] == 0).sum()
        print(f"  - Zero distance trips: {zero_distance} ({zero_distance/len(df)*100:.2f}%)")
    
    # Generate visualizations
    print("\n4. GENERATING VISUALIZATIONS...")
    generate_visualizations(df)
    
    return True

def generate_visualizations(df):
    """Create various visualizations from the data"""
    
    # Set up the plotting style
    plt.style.use('default')
    
    # Create a directory for visualizations
    os.makedirs("visualizations", exist_ok=True)
    
    # 1. Trip distance distribution
    if 'trip_distance' in df.columns:
        plt.figure(figsize=(10, 6))
        # Filter extreme values for better visualization
        filtered_distances = df[df['trip_distance'] <= 50]['trip_distance']
        plt.hist(filtered_distances, bins=50, edgecolor='black', alpha=0.7)
        plt.title('Distribution of Trip Distances')
        plt.xlabel('Trip Distance (miles)')
        plt.ylabel('Frequency')
        plt.grid(True, alpha=0.3)
        plt.savefig('visualizations/trip_distance_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("  - Created trip distance distribution plot")
    
    # 2. Passenger count distribution
    if 'passenger_count' in df.columns:
        plt.figure(figsize=(10, 6))
        passenger_counts = df['passenger_count'].value_counts().sort_index()
        passenger_counts.plot(kind='bar', edgecolor='black', alpha=0.7)
        plt.title('Distribution of Passenger Counts')
        plt.xlabel('Number of Passengers')
        plt.ylabel('Frequency')
        plt.grid(True, alpha=0.3)
        plt.savefig('visualizations/passenger_count_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("  - Created passenger count distribution plot")
    
    # 3. Payment type distribution
    if 'payment_type' in df.columns:
        plt.figure(figsize=(10, 6))
        payment_types = df['payment_type'].value_counts()
        plt.pie(payment_types.values, labels=payment_types.index, autopct='%1.1f%%')
        plt.title('Payment Type Distribution')
        plt.savefig('visualizations/payment_type_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("  - Created payment type distribution plot")
    
    # 4. Fare amount vs trip distance
    if all(col in df.columns for col in ['fare_amount', 'trip_distance']):
        plt.figure(figsize=(10, 6))
        # Sample the data to avoid overplotting
        sample_df = df.sample(min(1000, len(df)))
        plt.scatter(sample_df['trip_distance'], sample_df['fare_amount'], alpha=0.6)
        plt.title('Fare Amount vs Trip Distance')
        plt.xlabel('Trip Distance (miles)')
        plt.ylabel('Fare Amount ($)')
        plt.grid(True, alpha=0.3)
        plt.savefig('visualizations/fare_vs_distance.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("  - Created fare amount vs trip distance plot")
    
    # 5. Time-based analysis (if datetime columns exist)
    datetime_cols = [col for col in df.columns if 'datetime' in col.lower() or 'time' in col.lower()]
    if datetime_cols:
        # Use the first datetime column found
        time_col = datetime_cols[0]
        try:
            # Convert to datetime
            df[time_col] = pd.to_datetime(df[time_col])
            
            # Extract hour of day
            df['hour'] = df[time_col].dt.hour
            
            # Plot trips by hour
            plt.figure(figsize=(10, 6))
            trips_by_hour = df['hour'].value_counts().sort_index()
            trips_by_hour.plot(kind='bar', edgecolor='black', alpha=0.7)
            plt.title('Number of Trips by Hour of Day')
            plt.xlabel('Hour of Day')
            plt.ylabel('Number of Trips')
            plt.grid(True, alpha=0.3)
            plt.savefig('visualizations/trips_by_hour.png', dpi=300, bbox_inches='tight')
            plt.close()
            print("  - Created trips by hour of day plot")
            
        except Exception as e:
            print(f"  - Could not process datetime column: {e}")
    
    print("  - All visualizations saved to 'visualizations' folder")

def main():
    print("NYC Yellow Taxi Data Analysis")
    print("=" * 50)
    
    # Define the file to analyze
    hdfs_path = "/MIT805A1/combined_all_yellow_taxi_data/nyc_all_yellow_taxi_data_2023_2025_combined.csv"
    
    # Check if file exists in HDFS
    exists, _ = run_command(f"hdfs dfs -test -e {hdfs_path}")
    if not exists:
        print(f"❌ File not found: {hdfs_path}")
        print("Please make sure the combined file exists in HDFS")
        return
    
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Using temporary directory: {temp_dir}")
        
        # Download a sample of the data
        df = download_sample_file(hdfs_path, temp_dir, sample_size=5000)
        
        if df is not None:
            # Perform analysis and generate visualizations
            success = analyze_data(df)
            
            if success:
                print("\n✅ Analysis complete! Check the 'visualizations' folder for charts.")
            else:
                print("\n❌ Analysis failed!")
        else:
            print("❌ Failed to download and process data!")

if __name__ == "__main__":
    main()