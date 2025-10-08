# generate_statistics_silent.py
import subprocess
import pandas as pd
import tempfile
import os
from datetime import datetime

def run_hdfs_command(command, timeout=60):
    """Run HDFS command and return result"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=timeout)
        return result
    except:
        return None

def get_file_header(hdfs_path):
    """Get the header row from CSV file"""
    result = run_hdfs_command(f"hdfs dfs -cat {hdfs_path} | findstr /n \"^\" | findstr \"^1:\"")
    if result and result.returncode == 0:
        header = result.stdout.strip().replace('1:', '', 1)
        return header.split(',')
    return None

def estimate_row_count(hdfs_path):
    """Estimate total row count using file size"""
    result = run_hdfs_command(f"hdfs dfs -du {hdfs_path}")
    if result and result.returncode == 0:
        parts = result.stdout.strip().split()
        if parts:
            file_size = int(parts[0])
            avg_row_size = 250
            estimated_rows = file_size // avg_row_size
            return estimated_rows, file_size
    return 0, 0

def download_sample_data(hdfs_path, sample_lines=1000):
    """Download sample data from HDFS"""
    temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv', encoding='utf-8')
    temp_path = temp_file.name
    temp_file.close()
    
    result = run_hdfs_command(f"hdfs dfs -cat {hdfs_path} | head -n {sample_lines} > {temp_path}")
    
    if result and result.returncode == 0 and os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
        return temp_path
    return None

def generate_and_save_statistics():
    """Generate statistics and save to local drive"""
    
    # Set output directory
    output_dir = r"C:\MIT805_A1_Data\data"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "dataset_statistics_report.txt")
    
    # Find the combined CSV file
    hdfs_path = "/MIT805A1/combined_all_taxi_data/nyc_all_taxi_data_2023_2024_combined.csv"
    
    # Verify file exists
    result = run_hdfs_command(f"hdfs dfs -test -e {hdfs_path}")
    if result and result.returncode != 0:
        # Try to find other files
        result = run_hdfs_command("hdfs dfs -ls /MIT805A1/*.csv")
        if result and result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            for line in lines:
                if '.csv' in line:
                    parts = line.split()
                    if len(parts) >= 8:
                        hdfs_path = parts[-1]
                        break
    
    # Get file information
    total_rows, file_size = estimate_row_count(hdfs_path)
    file_size_gb = file_size / (1024 ** 3) if file_size > 0 else 0
    
    # Get column information
    columns = get_file_header(hdfs_path)
    num_variables = len(columns) if columns else 0
    
    # Download sample data
    sample_path = download_sample_data(hdfs_path, 5000)
    
    if sample_path and os.path.exists(sample_path):
        try:
            df = pd.read_csv(sample_path, low_memory=False)
            sample_rows = len(df)
            
            missing_cells = df.isnull().sum().sum()
            missing_percentage = (missing_cells / (num_variables * sample_rows)) * 100 if sample_rows > 0 else 0
            
            duplicate_rows = sample_rows - len(df.drop_duplicates())
            duplicate_percentage = (duplicate_rows / sample_rows) * 100 if sample_rows > 0 else 0
            
            sample_memory = df.memory_usage(deep=True).sum()
            avg_record_size = sample_memory / sample_rows if sample_rows > 0 else 0
            total_memory_estimate = avg_record_size * total_rows
            
            # Format memory sizes
            def format_size(size_bytes):
                for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                    if size_bytes < 1024.0:
                        return f"{size_bytes:.1f} {unit}"
                    size_bytes /= 1024.0
                return f"{size_bytes:.1f} PB"
            
            total_memory_readable = format_size(total_memory_estimate)
            avg_record_size_readable = format_size(avg_record_size)
            
            statistics_data = [
                ("Number of variables", f"{num_variables}", ""),
                ("Number of observations", f"{total_rows:,}", ""),
                ("Missing cells", f"{missing_cells:,}", f"{missing_percentage:.1f}%"),
                ("Duplicate rows", f"{duplicate_rows:,}", f"{duplicate_percentage:.1f}%"),
                ("Total size in memory", total_memory_readable, ""),
                ("Average record size in memory", avg_record_size_readable, "")
            ]
            
            save_statistics_to_file(output_file, statistics_data, hdfs_path, file_size_gb, columns)
            
        except Exception as e:
            # Fallback to basic statistics
            statistics_data = [
                ("Number of variables", f"{num_variables}", ""),
                ("Number of observations", f"{total_rows:,}", ""),
                ("Missing cells", "N/A", "N/A"),
                ("Duplicate rows", "N/A", "N/A"),
                ("Total size in memory", f"{file_size_gb:.2f} GB", ""),
                ("Average record size in memory", "N/A", "")
            ]
            save_statistics_to_file(output_file, statistics_data, hdfs_path, file_size_gb, columns)
        
        finally:
            if os.path.exists(sample_path):
                os.unlink(sample_path)
    else:
        # Basic statistics without sample data
        statistics_data = [
            ("Number of variables", f"{num_variables}", ""),
            ("Number of observations", f"{total_rows:,}", ""),
            ("Missing cells", "N/A", "N/A"),
            ("Duplicate rows", "N/A", "N/A"),
            ("Total size in memory", f"{file_size_gb:.2f} GB", ""),
            ("Average record size in memory", "N/A", "")
        ]
        save_statistics_to_file(output_file, statistics_data, hdfs_path, file_size_gb, columns)

def save_statistics_to_file(output_file, statistics_data, hdfs_path, file_size_gb, columns):
    """Save statistics to the specified file"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("Dataset statistics\n\n")
        
        f.write(f"{'Number of variables':<25} {statistics_data[0][1]}\n")
        f.write(f"{'Number of observations':<25} {statistics_data[1][1]}\n")
        f.write(f"{'Missing cells':<25} {statistics_data[2][1]}\n")
        f.write(f"{'Missing cells (%)':<25} {statistics_data[2][2]}\n")
        f.write(f"{'Duplicate rows':<25} {statistics_data[3][1]}\n")
        f.write(f"{'Duplicate rows (%)':<25} {statistics_data[3][2]}\n")
        f.write(f"{'Total size in memory':<25} {statistics_data[4][1]}\n")
        f.write(f"{'Average record size in memory':<25} {statistics_data[5][1]}\n\n")
        
        f.write(f"Generated on: {timestamp}\n")
        f.write(f"HDFS Path: {hdfs_path}\n")
        f.write(f"File Size: {file_size_gb:.2f} GB\n")
        
        if columns:
            f.write(f"\nColumns: {len(columns)}\n")

if __name__ == "__main__":
    # Run silently - no output to console
    generate_and_save_statistics()