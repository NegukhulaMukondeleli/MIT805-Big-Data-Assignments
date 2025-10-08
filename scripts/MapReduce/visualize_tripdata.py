#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import os

# Base folder for MapReduce outputs
BASE = "/home/negukhula"

# Map of output directories and expected chart info
outputs = {
    "output_trips_per_day": ("Trips per Day", "Date", "Trip Count"),
    "output_fare_per_day": ("Total Fare per Day", "Date", "Total Fare (USD)"),
    "output_trips_per_pulocation": ("Trips per Pickup Location (Top 20)", "Pickup Location ID", "Trip Count"),
    "output_trips_per_payment": ("Trips per Payment Type", "Payment Type", "Trip Count"),
}

save_dir = os.path.join(BASE, "visualizations")
os.makedirs(save_dir, exist_ok=True)

for folder, (title, xlabel, ylabel) in outputs.items():
    path = os.path.join(BASE, folder, "part-00000")
    if not os.path.exists(path):
        print(f"⚠️ Skipping {folder} — no output found.")
        continue

    # Read output
    try:
        df = pd.read_csv(path, sep="\t", header=None, names=["key", "value"])
    except Exception as e:
        print(f"Error reading {path}: {e}")
        continue

    # Choose visualization type
    if "day" in folder:
        # Sort by date
        df = df.sort_values("key")
        plt.figure(figsize=(10,5))
        plt.plot(df["key"], df["value"], marker="o")
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.tight_layout()

    elif "pulocation" in folder:
        # Top 20 busiest locations
        df = df.sort_values("value", ascending=False).head(20)
        plt.figure(figsize=(10,5))
        plt.bar(df["key"].astype(str), df["value"])
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.xticks(rotation=90)
        plt.tight_layout()

    elif "payment" in folder:
        # Payment type distribution
        plt.figure(figsize=(6,6))
        plt.pie(df["value"], labels=df["key"], autopct="%1.1f%%", startangle=140)
        plt.title(title)
        plt.tight_layout()

    # Save plot
    save_path = os.path.join(save_dir, f"{folder}.png")
    plt.savefig(save_path)
    plt.close()
    print(f"✅ Saved: {save_path}")

print(f"\n🎨 All visualizations saved in: {save_dir}")
