# visualize_taxi_data.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Set up visualization style
plt.style.use('ggplot')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (12, 8)

def load_sample_data():
    """Load a sample of the data for visualization"""
    print("Loading sample data...")
    
    # Load one month as sample (you can change this to any month)
    sample_file = "../data/yellow_tripdata_2023-01.parquet"
    df = pd.read_parquet(sample_file)
    
    print(f"Loaded {len(df):,} rows from January 2023")
    return df

def create_trip_distance_histogram(df):
    """Bar chart of trip distance distribution"""
    print("Creating trip distance histogram...")
    
    plt.figure(figsize=(14, 6))
    
    # Filter extreme values for better visualization
    filtered_df = df[(df['trip_distance'] > 0) & (df['trip_distance'] <= 20)]
    
    plt.hist(filtered_df['trip_distance'], bins=50, alpha=0.7, edgecolor='black')
    plt.title('Distribution of Trip Distances (0-20 miles)')
    plt.xlabel('Trip Distance (miles)')
    plt.ylabel('Frequency')
    plt.grid(True, alpha=0.3)
    plt.savefig('trip_distance_histogram.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_payment_type_pie_chart(df):
    """Pie chart of payment methods"""
    print("Creating payment type pie chart...")
    
    payment_types = {
        1: 'Credit Card',
        2: 'Cash',
        3: 'No Charge',
        4: 'Dispute',
        5: 'Unknown',
        6: 'Voided Trip'
    }
    
    payment_counts = df['payment_type'].value_counts()
    payment_labels = [payment_types.get(idx, f'Unknown {idx}') for idx in payment_counts.index]
    
    plt.figure(figsize=(10, 8))
    plt.pie(payment_counts.values, labels=payment_labels, autopct='%1.1f%%', startangle=90)
    plt.title('Payment Methods Used in Taxi Trips')
    plt.savefig('payment_type_pie_chart.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_passenger_count_bar_chart(df):
    """Bar chart of passenger counts"""
    print("Creating passenger count bar chart...")
    
    passenger_counts = df['passenger_count'].value_counts().sort_index()
    
    plt.figure(figsize=(12, 6))
    passenger_counts.plot(kind='bar', color='skyblue', edgecolor='black')
    plt.title('Number of Trips by Passenger Count')
    plt.xlabel('Number of Passengers')
    plt.ylabel('Number of Trips')
    plt.xticks(rotation=0)
    plt.grid(axis='y', alpha=0.3)
    
    # Add value labels on bars
    for i, v in enumerate(passenger_counts):
        plt.text(i, v + 1000, str(v), ha='center', va='bottom')
    
    plt.savefig('passenger_count_bar_chart.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_correlation_heatmap(df):
    """Correlation heatmap of numerical features"""
    print("Creating correlation heatmap...")
    
    # Select numerical columns for correlation
    numerical_cols = ['trip_distance', 'fare_amount', 'extra', 'mta_tax', 
                     'tip_amount', 'tolls_amount', 'total_amount']
    
    # Calculate correlation matrix
    corr_matrix = df[numerical_cols].corr()
    
    plt.figure(figsize=(12, 8))
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0, 
                square=True, fmt='.2f', linewidths=0.5)
    plt.title('Correlation Heatmap of Trip Metrics')
    plt.savefig('correlation_heatmap.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_hourly_trip_bar_chart(df):
    """Bar chart of trips by hour of day"""
    print("Creating hourly trip bar chart...")
    
    # Extract hour from datetime
    df['pickup_hour'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.hour
    
    hourly_counts = df['pickup_hour'].value_counts().sort_index()
    
    plt.figure(figsize=(14, 6))
    hourly_counts.plot(kind='bar', color='lightcoral', edgecolor='black')
    plt.title('Number of Trips by Hour of Day')
    plt.xlabel('Hour of Day')
    plt.ylabel('Number of Trips')
    plt.xticks(rotation=0)
    plt.grid(axis='y', alpha=0.3)
    plt.savefig('hourly_trip_bar_chart.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_fare_amount_vs_distance_scatter(df):
    """Scatter plot of fare amount vs trip distance"""
    print("Creating fare vs distance scatter plot...")
    
    # Sample the data for better performance
    sample_df = df.sample(n=10000, random_state=42)
    
    plt.figure(figsize=(12, 8))
    plt.scatter(sample_df['trip_distance'], sample_df['total_amount'], 
                alpha=0.6, s=10, color='green')
    plt.title('Total Amount vs Trip Distance')
    plt.xlabel('Trip Distance (miles)')
    plt.ylabel('Total Amount ($)')
    plt.grid(True, alpha=0.3)
    
    # Add trend line
    z = np.polyfit(sample_df['trip_distance'], sample_df['total_amount'], 1)
    p = np.poly1d(z)
    plt.plot(sample_df['trip_distance'], p(sample_df['trip_distance']), "r--", alpha=0.8)
    
    plt.savefig('fare_vs_distance_scatter.png', dpi=300, bbox_inches='tight')
    plt.show()

def main():
    """Main function to run all visualizations"""
    print("Starting NYC Yellow Taxi Data Visualization")
    print("=" * 50)
    
    # Load data
    df = load_sample_data()
    
    # Create visualizations
    create_trip_distance_histogram(df)
    create_payment_type_pie_chart(df)
    create_passenger_count_bar_chart(df)
    create_correlation_heatmap(df)
    create_hourly_trip_bar_chart(df)
    create_fare_amount_vs_distance_scatter(df)
    
    print("=" * 50)
    print("All visualizations completed!")
    print("Check the generated PNG files in your current directory.")

if __name__ == "__main__":
    main()