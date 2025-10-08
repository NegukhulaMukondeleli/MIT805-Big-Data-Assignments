import pandas as pd
import glob

files = glob.glob(r"C:\MIT805_A1_Data\data\yellow_trip_local\*.parquet")

dfs = [pd.read_parquet(f) for f in files]
big_df = pd.concat(dfs, ignore_index=True)
big_df.to_csv(r"C:\MIT805_A1_Data\data\yellow_tripdata_combined.csv", index=False)

print("Done! Combined CSV saved.")
