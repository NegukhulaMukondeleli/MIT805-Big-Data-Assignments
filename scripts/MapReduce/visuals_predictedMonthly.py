#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
from prophet import Prophet
from datetime import datetime

# === Load Data ===
path = "/home/negukhula/output_trips_per_day/part-00000"

# Skip header if it exists, handle both cases safely
df = pd.read_csv(path, sep="\t", comment="#", header=0, names=["Date", "count_trips_per_day"])

# Drop rows that are clearly headers or invalid
df = df[df["Date"].str.match(r"\d{4}-\d{2}-\d{2}", na=False)]

# Parse dates explicitly
df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d", errors="coerce")
df = df.dropna(subset=["Date"])
df = df.sort_values("Date")
df["count_trips_per_day"] = pd.to_numeric(df["count_trips_per_day"], errors="coerce").fillna(0)

# === Aggregate to Monthly Totals ===
df_monthly = df.resample("MS", on="Date").sum().reset_index()
df_monthly.rename(columns={"Date": "ds", "count_trips_per_day": "y"}, inplace=True)

# Add cap and floor for logistic growth
y_max = df_monthly["y"].max()
df_monthly["cap"] = y_max * 1.1
df_monthly["floor"] = df_monthly["y"].min() * 0.9

# === Define Conservative Prophet Model ===
model = Prophet(
    growth="logistic",
    yearly_seasonality=True,
    weekly_seasonality=False,
    daily_seasonality=False,
    changepoint_prior_scale=0.05,
    seasonality_mode="additive"
)
model.fit(df_monthly)

# === Predict Future (June 2025 – March 2026) ===
future = model.make_future_dataframe(periods=12, freq="MS")
future["cap"] = y_max * 1.1
future["floor"] = df_monthly["y"].min() * 0.9
forecast = model.predict(future)

# Keep only needed columns
forecast = forecast[["ds", "yhat"]]

# === Merge Actual + Predicted ===
merged = pd.merge(df_monthly, forecast, on="ds", how="outer")
merged["month_str"] = merged["ds"].dt.strftime("%Y-%m")
merged["actual"] = merged["y"]
merged["predicted"] = merged["yhat"]
merged = merged[["month_str", "actual", "predicted"]]

# === Focus Range: June 2025 – March 2026 ===
focus = merged[
    (merged["month_str"] >= "2025-06") & (merged["month_str"] <= "2026-03")
]

# === Clamp predictions within realistic range ===
focus["predicted"] = focus["predicted"].clip(
    lower=focus["actual"].min() * 0.8,
    upper=focus["actual"].max() * 1.1,
)

# === Plot Actual vs Predicted ===
plt.figure(figsize=(10, 6))
plt.plot(focus["month_str"], focus["actual"], label="Actual", color="green", marker="o")
plt.plot(focus["month_str"], focus["predicted"], label="Predicted", color="red", linestyle="--", marker="x")
plt.title("NYC Yellow Taxi Trips: Actual vs Predicted (Jun 2025 – Mar 2026)")
plt.xlabel("Month")
plt.ylabel("Total Trips per Month")
plt.xticks(rotation=45)
plt.grid(True)
plt.legend()
plt.tight_layout()

save_path = "/home/negukhula/visualizations/trip_forecast_conservative.png"
plt.savefig(save_path)
plt.close()

# === Display Comparison Table ===
print("📊 Forecast Comparison (June 2025 – March 2026):\n")
print(focus.to_string(index=False))
print(f"\n✅ Visualization saved to: {save_path}")
