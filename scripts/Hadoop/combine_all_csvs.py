# combine_all_taxi_data.py
import pandas as pd
import subprocess
import tempfile
import os

def run_command(command):
    """Run a command and return success status and output"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        return result.returncode == 0, result.stdout
    except Exception as e:
        return False, str(e)

def download_and_process_file(hdfs_path, source_type, temp_dir):
    """Download a file from HDFS, add source column, and return DataFrame"""
    print(f"Processing {source_type} data: {hdfs_path}")
    
    local_path = os.path.join(temp_dir, f"temp_{source_type}.csv")
    
    # Download from HDFS
    success, output = run_command(f"hdfs dfs -get {hdfs_path} {local_path}")
    if not success:
        print(f"❌ Failed to download {hdfs_path}: {output}")
        return None
    
    # Read the CSV file in chunks
    print(f"Reading {source_type} data...")
    try:
        # Read the file in chunks to handle large files
        chunk_size = 100000
        chunks = []
        
        for i, chunk in enumerate(pd.read_csv(local_path, chunksize=chunk_size)):
            chunk['file_source'] = source_type
            chunks.append(chunk)
            print(f"  Processed chunk {i+1}: {len(chunk)} rows")
        
        # Combine all chunks
        df = pd.concat(chunks, ignore_index=True)
        print(f"✅ Processed {len(df):,} rows from {source_type} data")
        return df
        
    except Exception as e:
        print(f"❌ Error processing {source_type} data: {e}")
        return None

def main():
    print("Combining all taxi data (Yellow + Green) with file_source column")
    print("=" * 70)
    
    # Define the files to combine
    files_to_combine = [
        ("/MIT805A1/nyc_taxi_2023_combined_yellow.csv", "yellow"),
        ("/MIT805A1/nyc_taxi_2024_combined_yellow.csv", "yellow"),
        ("/MIT805A1/nyc_taxi_2025_combined_yellow.csv", "yellow")
    ]
    
    # Create output directory in HDFS
    output_dir = "/MIT805A1/combined_all_yellow_taxi_data/"
    run_command(f"hdfs dfs -mkdir -p {output_dir}")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        all_dataframes = []
        total_rows = 0
        
        # Process each file
        for hdfs_path, source_type in files_to_combine:
            # Check if file exists in HDFS
            exists, _ = run_command(f"hdfs dfs -test -e {hdfs_path}")
            if not exists:
                print(f"⚠️  File not found, skipping: {hdfs_path}")
                continue
            
            df = download_and_process_file(hdfs_path, source_type, temp_dir)
            if df is not None:
                all_dataframes.append(df)
                total_rows += len(df)
        
        if not all_dataframes:
            print("❌ No data to combine!")
            return
        
        # Combine all DataFrames
        print(f"\nCombining all data... (Total: {total_rows:,} rows)")
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Save the combined file
        output_filename = "nyc_all_yellow_taxi_data_2023_2025_combined.csv"
        local_output_path = os.path.join(temp_dir, output_filename)
        
        print(f"Saving combined file with {len(combined_df):,} rows...")
        combined_df.to_csv(local_output_path, index=False)
        
        # Upload to HDFS
        hdfs_output_path = f"{output_dir}{output_filename}"
        print(f"Uploading to HDFS: {hdfs_output_path}")
        
        success, output = run_command(f"hdfs dfs -put {local_output_path} {hdfs_output_path}")
        if success:
            print(f"✅ Successfully created combined file: {hdfs_output_path}")
            
            # Show file statistics
            file_size = os.path.getsize(local_output_path) / (1024**3)  # GB
            print(f"📊 File size: {file_size:.2f} GB")
            print(f"📊 Total rows: {len(combined_df):,}")
            print(f"📊 Columns: {len(combined_df.columns)}")
            
            # Show source distribution
            source_dist = combined_df['file_source'].value_counts()
            print(f"📊 Source distribution:")
            for source, count in source_dist.items():
                print(f"   {source}: {count:,} rows ({count/len(combined_df)*100:.1f}%)")
            
            # Verify the upload
            run_command(f"hdfs dfs -ls {output_dir}")
            run_command(f"hdfs dfs -du -h {hdfs_output_path}")
            
        else:
            print(f"❌ Upload failed: {output}")

if __name__ == "__main__":
    main()