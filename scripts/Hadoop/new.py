# combine_all_yellow_taxi_data.py
import pandas as pd
import subprocess
import os

# Fixed local temp directory
TEMP_DIR = r"C:\MIT805_A1_Data\temp"
os.makedirs(TEMP_DIR, exist_ok=True)

def run_command(command):
    """Run a shell command and return success status and output"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Command failed: {command}")
            print(f"stdout: {result.stdout}")
            print(f"stderr: {result.stderr}")
        return result.returncode == 0, result.stdout
    except Exception as e:
        return False, str(e)

def download_and_process_file(hdfs_path, source_type):
    """Download a file from HDFS, add source column, and return DataFrame"""
    print(f"Processing {source_type} data: {hdfs_path}")
    local_path = os.path.join(TEMP_DIR, f"temp_{source_type}.csv")

    # Download from HDFS
    success, _ = run_command(f"hdfs dfs -get {hdfs_path} {local_path}")
    if not success:
        print(f"❌ Failed to download {hdfs_path}")
        return None

    # Read CSV in chunks
    print(f"Reading {source_type} data in chunks...")
    try:
        chunk_size = 100_000
        chunks = []
        for i, chunk in enumerate(pd.read_csv(local_path, chunksize=chunk_size)):
            chunk['file_source'] = source_type
            chunks.append(chunk)
            print(f"  Processed chunk {i+1}: {len(chunk)} rows")
        df = pd.concat(chunks, ignore_index=True)
        print(f"✅ Processed {len(df):,} rows from {source_type}")
        return df
    except Exception as e:
        print(f"❌ Error reading {local_path}: {e}")
        return None

def main():
    print("Combining all yellow taxi data (2023-2025) with file_source column")
    print("=" * 70)

    # HDFS files to combine
    files_to_combine = [
        ("/MIT805A1/nyc_taxi_2023_combined_yellow.csv", "yellow_2023"),
        ("/MIT805A1/nyc_taxi_2024_combined_yellow.csv", "yellow_2024"),
        ("/MIT805A1/nyc_taxi_2025_combined_yellow.csv", "yellow_2025"),
    ]

    # HDFS output directory
    output_dir = "/MIT805A1/combined_all_yellow_taxi_data/"
    run_command(f"hdfs dfs -mkdir -p {output_dir}")

    all_dataframes = []
    total_rows = 0

    for hdfs_path, source_type in files_to_combine:
        # Check if file exists in HDFS
        exists, _ = run_command(f"hdfs dfs -test -e {hdfs_path}")
        if not exists:
            print(f"⚠️  File not found, skipping: {hdfs_path}")
            continue

        df = download_and_process_file(hdfs_path, source_type)
        if df is not None:
            all_dataframes.append(df)
            total_rows += len(df)

    if not all_dataframes:
        print("❌ No data to combine!")
        return

    # Combine all DataFrames
    print(f"\nCombining all data... (Total rows: {total_rows:,})")
    combined_df = pd.concat(all_dataframes, ignore_index=True)

    # Save locally
    output_filename = "nyc_all_yellow_taxi_data_2023_2025_combined.csv"
    local_output_path = os.path.join(TEMP_DIR, output_filename)
    combined_df.to_csv(local_output_path, index=False)
    print(f"✅ Saved combined file locally: {local_output_path}")

    # Upload to HDFS
    hdfs_output_path = os.path.join(output_dir, output_filename)
    success, _ = run_command(f"hdfs dfs -put -f {local_output_path} {hdfs_output_path}")
    if success:
        print(f"✅ Successfully uploaded to HDFS: {hdfs_output_path}")
        # File stats
        file_size = os.path.getsize(local_output_path) / (1024**3)  # GB
        print(f"📊 File size: {file_size:.2f} GB")
        print(f"📊 Total rows: {len(combined_df):,}")
        print(f"📊 Columns: {len(combined_df.columns)}")
        # Source distribution
        source_dist = combined_df['file_source'].value_counts()
        print(f"📊 Source distribution:")
        for source, count in source_dist.items():
            print(f"   {source}: {count:,} rows ({count/len(combined_df)*100:.1f}%)")
    else:
        print(f"❌ Failed to upload combined file to HDFS")

if __name__ == "__main__":
    main()
