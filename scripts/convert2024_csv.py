# convert2023_simple.py
import pandas as pd
import subprocess
import os
import tempfile

def run_command(command):
    """Run a command and return True if successful, False otherwise"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            return True
        else:
            print(f"Command failed: {command}")
            print(f"Error: {result.stderr}")
            return False
    except Exception as e:
        print(f"Exception running command: {e}")
        return False

def main():
    print("Creating 2023 CSV using local conversion...")
    
    # Create output directory in HDFS
    output_path = "/user/MukondeleliNegukhula/nyc_taxi/csv_yellow_2023/"
    run_command(f"hdfs dfs -mkdir -p {output_path}")
    
    # Process each 2023 file individually
    for month in range(1, 13):
        month_str = str(month).zfill(2)  # Convert 1 to '01', 2 to '02', etc.
        input_file = f"/user/MukondeleliNegukhula/nyc_taxi/raw/yellow_tripdata_2023-{month_str}.parquet"
        output_file = f"yellow_tripdata_2023-{month_str}.csv"
        
        print(f"Processing 2023-{month_str}...")
        
        # Check if file exists in HDFS
        if not run_command(f"hdfs dfs -test -e {input_file}"):
            print(f"⚠️  File not found: {input_file}")
            continue
        
        # Create a temporary directory for processing
        with tempfile.TemporaryDirectory() as temp_dir:
            local_parquet = os.path.join(temp_dir, f"temp_{month_str}.parquet")
            local_csv = os.path.join(temp_dir, output_file)
            
            # 1. Download from HDFS to local
            print(f"  Downloading {input_file}...")
            if not run_command(f"hdfs dfs -get {input_file} {local_parquet}"):
                continue
            
            # 2. Convert Parquet to CSV using pandas
            print(f"  Converting to CSV...")
            try:
                df = pd.read_parquet(local_parquet)
                df.to_csv(local_csv, index=False)
                print(f"  Converted to CSV: {len(df)} rows")
            except Exception as e:
                print(f"  ❌ Conversion failed: {e}")
                continue
            
            # 3. Upload CSV back to HDFS
            print(f"  Uploading to HDFS...")
            if run_command(f"hdfs dfs -put {local_csv} {output_path}"):
                print(f"  ✅ Successfully processed 2023-{month_str}")
            else:
                print(f"  ❌ Upload failed for 2023-{month_str}")
    
    print("✅ All 2023 files processed!")
    print(f"Final output in HDFS: {output_path}")
    
    # Show the final results
    print("\nFinal output files:")
    run_command(f"hdfs dfs -ls {output_path}")

if __name__ == "__main__":
    main()