#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import os

# File paths
BASE = "/home/negukhula"
INPUT_PATH = os.path.join(BASE, "output_trips_per_day", "part-00000")
SAVE_DIR = os.path.join(BASE, "visualizations")
os.makedirs(SAVE_DIR, exist_ok=True)

# Read CSV (with headers)
df = pd.read_csv(INPUT_PATH, sep="\t")

# Ensure correct column names and datetime conversion
df.columns = ["Date", "count_trips_per_day"]
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

# Extract month number and name
df["month_num"] = df["Date"].dt.month
df["month_name"] = df["Date"].dt.strftime("%B")

# Group by month
monthly_trips = df.groupby(["month_num", "month_name"])["count_trips_per_day"].sum().reset_index()

# Sort months in calendar order (1–12)
monthly_trips = monthly_trips.sort_values("month_num")

# Plot monthly bar chart
plt.figure(figsize=(10, 6))
plt.bar(monthly_trips["month_name"], monthly_trips["count_trips_per_day"], color="skyblue")
plt.title("Total Trips per Month", fontsize=14)
plt.xlabel("Month")
plt.ylabel("Total Trips")
plt.xticks(rotation=45)
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.tight_layout()

# Save plot
output_path = os.path.join(SAVE_DIR, "monthly_trips.png")
plt.savefig(output_path)
plt.close()

print(f"✅ Monthly trips bar chart saved at: {output_path}")
