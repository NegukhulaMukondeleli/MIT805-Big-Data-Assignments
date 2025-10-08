import pandas as pd
import matplotlib.pyplot as plt

# Load your MapReduce output (example: trips per day)
df_trips = pd.read_csv('/home/negukhula/output_trips_per_day.csv')  # adjust path if needed
df_trips['date'] = pd.to_datetime(df_trips['date'])

# --- Aggregate by month ---
df_monthly = df_trips.groupby(df_trips['date'].dt.to_period('M')).sum()
df_monthly.index = df_monthly.index.to_timestamp()  # convert PeriodIndex back to timestamp for plotting

plt.figure(figsize=(12,6))
df_monthly['passenger_count'].plot(kind='bar', color='skyblue', alpha=0.7)
plt.title('Passenger Count per Month')
plt.ylabel('Passenger Count')
plt.xlabel('Month')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('/home/negukhula/visualizations/passenger_per_month.png')
plt.close()

plt.figure(figsize=(12,6))
df_monthly['trip_distance'].plot(kind='bar', color='salmon', alpha=0.7)
plt.title('Trip Distance per Month')
plt.ylabel('Trip Distance')
plt.xlabel('Month')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('/home/negukhula/visualizations/trip_distance_per_month.png')
plt.close()

# --- Aggregate by quarter ---
df_quarterly = df_trips.groupby(df_trips['date'].dt.to_period('Q')).sum()
df_quarterly.index = df_quarterly.index.to_timestamp()

plt.figure(figsize=(12,6))
df_quarterly['passenger_count'].plot(kind='bar', color='lightgreen', alpha=0.7)
plt.title('Passenger Count per Quarter')
plt.ylabel('Passenger Count')
plt.xlabel('Quarter')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('/home/negukhula/visualizations/passenger_per_quarter.png')
plt.close()

plt.figure(figsize=(12,6))
df_quarterly['trip_distance'].plot(kind='bar', color='orange', alpha=0.7)
plt.title('Trip Distance per Quarter')
plt.ylabel('Trip Distance')
plt.xlabel('Quarter')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('/home/negukhula/visualizations/trip_distance_per_quarter.png')
plt.close()
