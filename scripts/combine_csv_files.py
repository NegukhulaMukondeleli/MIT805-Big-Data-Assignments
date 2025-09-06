# combine_csv_to_MIT805A1.py
import pandas as pd
import subprocess
import os
import tempfile
from datetime import datetime

def run_command(command):
    """Run a command and return True if successful, False otherwise"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        return result.returncode == 0
    except Exception as e:
        print(f"Exception running command: {e}")
        return False

def extract_date_from_filename(filename):
    """Extract date from filename like yellow_tripdata_2023-01.csv"""
    try:
        # Extract the date part from filename
        date_str = filename.split('_')[-1].replace('.csv', '')
        year, month = date_str.split('-')
        return f"{year}-{month}-01"  # Return as YYYY-MM-DD format
    except:
        return None

def main():
    print("Combining 2023 CSV files and saving to /MIT805A1 folder...")
    
    # Create the /MIT805A1 directory in HDFS if it doesn't exist
    output_path = "/MIT805A1/"
    run_command(f"hdfs dfs -mkdir -p {output_path}")
    
    # Create a temporary directory for processing
    with tempfile.TemporaryDirectory() as temp_dir:
        all_data_frames = []
        
        # List all CSV files in the HDFS directory
        print("Listing CSV files in HDFS...")
        result = subprocess.run(f"hdfs dfs -ls /user/MukondeleliNegukhula/nyc_taxi/csv_yellow_2023/", 
                              shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print("❌ Failed to list HDFS files")
            return
        
        # Parse the output to get file names
        lines = result.stdout.strip().split('\n')
        csv_files = []
        for line in lines:
            if line.strip() and '.csv' in line:
                parts = line.split()
                if len(parts) >= 8:
                    csv_files.append(parts[-1])
        
        print(f"Found {len(csv_files)} CSV files to process")
        
        # Process each CSV file
        for hdfs_file_path in csv_files:
            filename = os.path.basename(hdfs_file_path)
            print(f"Processing {filename}...")
            
            # Extract date from filename
            filedate = extract_date_from_filename(filename)
            if not filedate:
                print(f"  ⚠️  Could not extract date from filename: {filename}")
                continue
            
            local_csv = os.path.join(temp_dir, filename)
            
            # 1. Download from HDFS to local
            if not run_command(f"hdfs dfs -get {hdfs_file_path} {local_csv}"):
                print(f"  ❌ Failed to download {filename}")
                continue
            
            # 2. Read CSV and add filedate column
            try:
                print(f"  Reading {filename}...")
                df = pd.read_csv(local_csv)
                df['filedate'] = filedate  # Add the new column
                all_data_frames.append(df)
                print(f"  ✅ Added {len(df)} rows with filedate: {filedate}")
            except Exception as e:
                print(f"  ❌ Failed to process {filename}: {e}")
                continue
        
        if not all_data_frames:
            print("❌ No data frames to combine")
            return
        
        # 3. Combine all DataFrames
        print("Combining all data...")
        combined_df = pd.concat(all_data_frames, ignore_index=True)
        
        # 4. Save combined CSV
        combined_filename = "nyc_taxi_2023_combined_yellow.csv"
        local_combined_path = os.path.join(temp_dir, combined_filename)
        
        print(f"Saving combined file with {len(combined_df)} rows...")
        combined_df.to_csv(local_combined_path, index=False)
        
        # 5. Upload combined file to /MIT805A1 folder in HDFS
        hdfs_final_path = f"{output_path}{combined_filename}"
        print(f"Uploading to {hdfs_final_path}...")
        
        if run_command(f"hdfs dfs -put {local_combined_path} {hdfs_final_path}"):
            print(f"✅ Successfully created combined file: {hdfs_final_path}")
            
            # Verify the file was uploaded
            print("Verifying upload...")
            run_command(f"hdfs dfs -ls {output_path}")
            run_command(f"hdfs dfs -du -h {hdfs_final_path}")
        else:
            print("❌ Failed to upload combined file")
        
        # Show some statistics
        print("\n📊 Combined File Statistics:")
        print(f"Total rows: {len(combined_df):,}")
        print(f"Total columns: {len(combined_df.columns)}")
        print(f"Date range: {combined_df['filedate'].min()} to {combined_df['filedate'].max()}")
        print(f"File size: {os.path.getsize(local_combined_path) / (1024*1024):.2f} MB")
        
        # Show sample of the data with the new field
        print("\nSample data with filedate:")
        print(combined_df[['filedate'] + list(combined_df.columns[:3])].head())
        
        # Show the first few rows with the new filedate column
        print("\nFirst few rows with the new filedate column:")
        sample_cols = ['filedate'] + [col for col in combined_df.columns if col != 'filedate'][:4]
        print(combined_df[sample_cols].head())

if __name__ == "__main__":
    main()