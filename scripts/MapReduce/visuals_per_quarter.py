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

# Extract month number
df["month_num"] = df["Date"].dt.month

# Map months to quarters manually
def get_quarter(month):
    if month in [1, 2, 3]:
        return "Q1 (Jan–Mar)"
    elif month in [4, 5, 6]:
        return "Q2 (Apr–Jun)"
    elif month in [7, 8, 9]:
        return "Q3 (Jul–Sep)"
    else:
        return "Q4 (Oct–Dec)"

df["Quarter"] = df["month_num"].apply(get_quarter)

# Group by quarter and sum trips
quarterly_trips = df.groupby("Quarter")["count_trips_per_day"].sum().reset_index()

# Ensure order is Q1 → Q4
quarter_order = ["Q1 (Jan–Mar)", "Q2 (Apr–Jun)", "Q3 (Jul–Sep)", "Q4 (Oct–Dec)"]
quarterly_trips["Quarter"] = pd.Categorical(quarterly_trips["Quarter"], categories=quarter_order, ordered=True)
quarterly_trips = quarterly_trips.sort_values("Quarter")

# Plot quarterly bar chart
plt.figure(figsize=(8, 5))
plt.bar(quarterly_trips["Quarter"], quarterly_trips["count_trips_per_day"], color="lightgreen")
plt.title("Total Trips per Quarter", fontsize=14)
plt.xlabel("Quarter")
plt.ylabel("Total Trips")
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.tight_layout()

# Save plot
output_path = os.path.join(SAVE_DIR, "quarterly_trips.png")
plt.savefig(output_path)
plt.close()

print(f"✅ Quarterly trips bar chart saved at: {output_path}")
