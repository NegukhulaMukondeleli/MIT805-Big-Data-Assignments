# visualize_combined_2023.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from hdfs import InsecureClient
import warnings
warnings.filterwarnings('ignore')

# Set up HDFS client
client = InsecureClient('http://localhost:9870', user='MukondeleliNegukhula')

def read_csv_from_hdfs(hdfs_path, sample_size=10000000):
    """Read CSV file directly from HDFS with sampling"""
    print(f"Reading data from HDFS: {hdfs_path}")
    
    with client.read(hdfs_path) as reader:
        # Read in chunks for large files
        chunk_iter = pd.read_csv(reader, chunksize=sample_size, iterator=True)
        df = next(chunk_iter)  # Get first chunk
    
    print(f"✅ Loaded {len(df):,} sample rows from combined 2023 data")
    return df

def create_annual_trends(df):
    """Visualize trends across the entire year"""
    print("Creating annual trends visualization...")
    
    # Convert filedate to datetime
    df['filedate'] = pd.to_datetime(df['filedate'])
    df['month'] = df['filedate'].dt.month
    df['month_name'] = df['filedate'].dt.month_name()
    
    # Monthly trip counts
    monthly_counts = df['month_name'].value_counts().reindex([
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ])
    
    plt.figure(figsize=(14, 6))
    monthly_counts.plot(kind='bar', color='lightseagreen', edgecolor='black')
    plt.title('Monthly Trip Volume - 2023 Complete Year')
    plt.xlabel('Month')
    plt.ylabel('Number of Trips')
    plt.xticks(rotation=45)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig('monthly_trips_2023.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_seasonal_analysis(df):
    """Analyze seasonal patterns"""
    print("Creating seasonal analysis...")
    
    df['filedate'] = pd.to_datetime(df['filedate'])
    df['season'] = df['filedate'].dt.month.map({
        12: 'Winter', 1: 'Winter', 2: 'Winter',
        3: 'Spring', 4: 'Spring', 5: 'Spring',
        6: 'Summer', 7: 'Summer', 8: 'Summer',
        9: 'Fall', 10: 'Fall', 11: 'Fall'
    })
    
    seasonal_stats = df.groupby('season').agg({
        'total_amount': 'mean',
        'trip_distance': 'mean',
        'passenger_count': 'mean'
    }).round(2)
    
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    
    metrics = ['total_amount', 'trip_distance', 'passenger_count']
    titles = ['Average Fare by Season', 'Average Distance by Season', 'Average Passengers by Season']
    colors = ['lightcoral', 'lightblue', 'lightgreen']
    
    for i, (metric, title, color) in enumerate(zip(metrics, titles, colors)):
        seasonal_stats[metric].plot(kind='bar', ax=axes[i], color=color, edgecolor='black')
        axes[i].set_title(title)
        axes[i].set_ylabel(metric.replace('_', ' ').title())
        axes[i].tick_params(axis='x', rotation=45)
        axes[i].grid(alpha=0.3)
        
        # Add value labels
        for j, v in enumerate(seasonal_stats[metric]):
            axes[i].text(j, v + 0.1, str(v), ha='center', va='bottom')
    
    plt.tight_layout()
    plt.savefig('seasonal_analysis_2023.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_revenue_analysis(df):
    """Analyze revenue patterns across the year"""
    print("Creating revenue analysis...")
    
    monthly_revenue = df.groupby('filedate')['total_amount'].sum().reset_index()
    monthly_revenue['filedate'] = pd.to_datetime(monthly_revenue['filedate'])
    monthly_revenue['month'] = monthly_revenue['filedate'].dt.month_name()
    
    plt.figure(figsize=(14, 6))
    plt.bar(monthly_revenue['month'], monthly_revenue['total_amount'], 
            color='goldenrod', edgecolor='black', alpha=0.7)
    plt.title('Total Revenue by Month - 2023')
    plt.xlabel('Month')
    plt.ylabel('Total Revenue ($)')
    plt.xticks(rotation=45)
    plt.grid(axis='y', alpha=0.3)
    
    # Format y-axis to show millions
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1e6:.1f}M'))
    
    plt.tight_layout()
    plt.savefig('revenue_analysis_2023.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_peak_hours_analysis(df):
    """Analyze peak hours across the entire year"""
    print("Creating peak hours analysis...")
    
    df['pickup_hour'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.hour
    
    hourly_distribution = df['pickup_hour'].value_counts().sort_index()
    
    plt.figure(figsize=(14, 6))
    hourly_distribution.plot(kind='bar', color='coral', edgecolor='black')
    plt.title('Peak Hours Analysis - Entire 2023 Dataset')
    plt.xlabel('Hour of Day')
    plt.ylabel('Number of Trips')
    plt.xticks(rotation=0)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig('peak_hours_2023.png', dpi=300, bbox_inches='tight')
    plt.show()

def main():
    print("Visualizing Combined 2023 Yellow Taxi Data from HDFS")
    print("=" * 60)
    
    # Load combined data from HDFS
    hdfs_path = "/MIT805A1/nyc_taxi_2024_combined_yellow.csv"
    
    try:
        df = read_csv_from_hdfs(hdfs_path, sample_size=20000000)
        
        # Create comprehensive visualizations
        create_annual_trends(df)
        create_seasonal_analysis(df)
        create_revenue_analysis(df)
        create_peak_hours_analysis(df)
        
        print("=" * 60)
        print("✅ All visualizations completed for combined 2023 data!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure the combined file exists in HDFS:")
        print("hdfs dfs -ls /MIT805A1/")

if __name__ == "__main__":
    main()