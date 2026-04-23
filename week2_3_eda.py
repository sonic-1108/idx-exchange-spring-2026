import pandas as pd
import os

data_folder = "/Users/soniczhang/Desktop/IDX Exchange/MLS code"

sold_path = os.path.join(data_folder, "combined_sold_residential.csv")
listing_path = os.path.join(data_folder, "combined_listing_residential.csv")

sold = pd.read_csv(sold_path)
listings = pd.read_csv(listing_path)

# -----------------------------
# Helper function for EDA
# -----------------------------
def eda_report(df, name):
    print(f"\n{'='*60}")
    print(f"{name} DATASET REPORT")
    print(f"{'='*60}")

    # Shape
    print(f"\nShape: {df.shape}")

    # Columns
    print("\nColumns:")
    print(df.columns.tolist())

    # Data types
    print("\nData types:")
    print(df.dtypes)

    # Property types
    if "PropertyType" in df.columns:
        print("\nUnique PropertyType values:")
        print(df["PropertyType"].dropna().unique())

    # Missing summary
    missing_counts = df.isnull().sum()
    missing_pct = (missing_counts / len(df)) * 100

    missing_summary = pd.DataFrame({
        "column": df.columns,
        "missing_count": missing_counts.values,
        "missing_pct": missing_pct.values
    }).sort_values(by="missing_pct", ascending=False)

    print("\nTop missing columns:")
    print(missing_summary.head(20))

    # Columns with >90% missing
    high_missing = missing_summary[missing_summary["missing_pct"] > 90]
    print("\nColumns with >90% missing:")
    print(high_missing)

    # Numeric field summary
    numeric_fields = ["ClosePrice", "LivingArea", "DaysOnMarket"]
    existing_numeric_fields = [col for col in numeric_fields if col in df.columns]

    if existing_numeric_fields:
        print("\nNumeric distribution summary:")
        print(df[existing_numeric_fields].describe(percentiles=[0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]).T)

    return missing_summary, high_missing

# Run EDA
sold_missing_summary, sold_high_missing = eda_report(sold, "SOLD")
listing_missing_summary, listing_high_missing = eda_report(listings, "LISTINGS")

# Save summaries
sold_missing_summary.to_csv(os.path.join(data_folder, "sold_missing_summary.csv"), index=False)
sold_high_missing.to_csv(os.path.join(data_folder, "sold_high_missing_over_90.csv"), index=False)

listing_missing_summary.to_csv(os.path.join(data_folder, "listing_missing_summary.csv"), index=False)
listing_high_missing.to_csv(os.path.join(data_folder, "listing_high_missing_over_90.csv"), index=False)

# Save validated datasets again if needed
sold.to_csv(os.path.join(data_folder, "sold_validated.csv"), index=False)
listings.to_csv(os.path.join(data_folder, "listing_validated.csv"), index=False)

print("\nEDA files saved successfully.")