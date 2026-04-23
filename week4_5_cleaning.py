import pandas as pd
import os

data_folder = "/Users/soniczhang/Desktop/IDX Exchange/MLS code"

sold_path = os.path.join(data_folder, "sold_with_mortgage_rates.csv")
listing_path = os.path.join(data_folder, "listing_with_mortgage_rates.csv")

sold = pd.read_csv(sold_path)
listings = pd.read_csv(listing_path)

# ---------------------------------
# Helper function
# ---------------------------------
def clean_dataset(df, name):
    print(f"\n{'='*70}")
    print(f"CLEANING {name}")
    print(f"{'='*70}")

    before_rows = len(df)
    print(f"Rows before cleaning: {before_rows}")

    # -----------------------------
    # 1. Convert date columns
    # -----------------------------
    date_cols = [
        "CloseDate",
        "PurchaseContractDate",
        "ListingContractDate",
        "ContractStatusChangeDate"
    ]

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # -----------------------------
    # 2. Convert numeric columns
    # -----------------------------
    numeric_cols = [
        "ClosePrice",
        "ListPrice",
        "OriginalListPrice",
        "LivingArea",
        "LotSizeAcres",
        "BedroomsTotal",
        "BathroomsTotalInteger",
        "DaysOnMarket",
        "YearBuilt",
        "Latitude",
        "Longitude",
        "rate_30yr_fixed"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # -----------------------------
    # 3. Remove unnecessary columns
    # -----------------------------
    # 这里只先删掉明显全空的列；其他冗余列可以以后再补
    all_null_cols = df.columns[df.isnull().all()].tolist()
    if all_null_cols:
        print("\nDropping fully null columns:")
        print(all_null_cols)
        df = df.drop(columns=all_null_cols)

    # -----------------------------
    # 4. Handle missing values
    # -----------------------------
    # 这里只保留缺失，不乱填值；主要做 flag/check
    missing_summary = df.isnull().sum().reset_index()
    missing_summary.columns = ["column", "missing_count"]
    missing_summary["missing_pct"] = missing_summary["missing_count"] / len(df) * 100

    # -----------------------------
    # 5. Invalid numeric value flags
    # -----------------------------
    if "ClosePrice" in df.columns:
        df["invalid_closeprice_flag"] = df["ClosePrice"] <= 0

    if "LivingArea" in df.columns:
        df["invalid_livingarea_flag"] = df["LivingArea"] <= 0

    if "DaysOnMarket" in df.columns:
        df["invalid_daysonmarket_flag"] = df["DaysOnMarket"] < 0

    if "BedroomsTotal" in df.columns:
        df["invalid_bedrooms_flag"] = df["BedroomsTotal"] < 0

    if "BathroomsTotalInteger" in df.columns:
        df["invalid_bathrooms_flag"] = df["BathroomsTotalInteger"] < 0

    # -----------------------------
    # 6. Date consistency flags
    # -----------------------------
    if {"ListingContractDate", "CloseDate"}.issubset(df.columns):
        df["listing_after_close_flag"] = df["ListingContractDate"] > df["CloseDate"]
    else:
        df["listing_after_close_flag"] = False

    if {"PurchaseContractDate", "CloseDate"}.issubset(df.columns):
        df["purchase_after_close_flag"] = df["PurchaseContractDate"] > df["CloseDate"]
    else:
        df["purchase_after_close_flag"] = False

    if {"ListingContractDate", "PurchaseContractDate", "CloseDate"}.issubset(df.columns):
        df["negative_timeline_flag"] = (
            (df["ListingContractDate"] > df["PurchaseContractDate"]) |
            (df["PurchaseContractDate"] > df["CloseDate"])
        )
    else:
        df["negative_timeline_flag"] = False

    # -----------------------------
    # 7. Geographic data checks
    # -----------------------------
    if {"Latitude", "Longitude"}.issubset(df.columns):
        df["missing_coordinates_flag"] = df["Latitude"].isnull() | df["Longitude"].isnull()
        df["zero_coordinates_flag"] = (df["Latitude"] == 0) | (df["Longitude"] == 0)
        df["positive_longitude_flag"] = df["Longitude"] > 0

        # California rough bounds
        df["implausible_coordinates_flag"] = (
            (df["Latitude"] < 32) | (df["Latitude"] > 42) |
            (df["Longitude"] < -125) | (df["Longitude"] > -114)
        )
    else:
        df["missing_coordinates_flag"] = False
        df["zero_coordinates_flag"] = False
        df["positive_longitude_flag"] = False
        df["implausible_coordinates_flag"] = False

    # -----------------------------
    # 8. Optional row filtering
    # -----------------------------
    # Week 4-5 更稳妥的做法是：先 flag，不直接删太多
    # 这里只删除最明显不合理的记录
    if "ClosePrice" in df.columns:
        df = df[df["ClosePrice"].isnull() | (df["ClosePrice"] > 0)]

    if "LivingArea" in df.columns:
        df = df[df["LivingArea"].isnull() | (df["LivingArea"] > 0)]

    if "DaysOnMarket" in df.columns:
        df = df[df["DaysOnMarket"].isnull() | (df["DaysOnMarket"] >= 0)]

    if "BedroomsTotal" in df.columns:
        df = df[df["BedroomsTotal"].isnull() | (df["BedroomsTotal"] >= 0)]

    if "BathroomsTotalInteger" in df.columns:
        df = df[df["BathroomsTotalInteger"].isnull() | (df["BathroomsTotalInteger"] >= 0)]

    after_rows = len(df)
    print(f"Rows after cleaning: {after_rows}")
    print(f"Rows removed: {before_rows - after_rows}")

    # -----------------------------
    # 9. Summary tables
    # -----------------------------
    flag_summary = pd.DataFrame({
        "flag": [
            "listing_after_close_flag",
            "purchase_after_close_flag",
            "negative_timeline_flag",
            "missing_coordinates_flag",
            "zero_coordinates_flag",
            "positive_longitude_flag",
            "implausible_coordinates_flag"
        ],
        "count": [
            df["listing_after_close_flag"].sum(),
            df["purchase_after_close_flag"].sum(),
            df["negative_timeline_flag"].sum(),
            df["missing_coordinates_flag"].sum(),
            df["zero_coordinates_flag"].sum(),
            df["positive_longitude_flag"].sum(),
            df["implausible_coordinates_flag"].sum()
        ]
    })

    return df, missing_summary, flag_summary

# Run cleaning
sold_cleaned, sold_missing_summary, sold_flag_summary = clean_dataset(sold, "SOLD")
listing_cleaned, listing_missing_summary, listing_flag_summary = clean_dataset(listings, "LISTINGS")

# Save outputs
sold_cleaned.to_csv(os.path.join(data_folder, "sold_cleaned_analysis_ready.csv"), index=False)
listing_cleaned.to_csv(os.path.join(data_folder, "listing_cleaned_analysis_ready.csv"), index=False)

sold_missing_summary.to_csv(os.path.join(data_folder, "sold_cleaning_missing_summary.csv"), index=False)
listing_missing_summary.to_csv(os.path.join(data_folder, "listing_cleaning_missing_summary.csv"), index=False)

sold_flag_summary.to_csv(os.path.join(data_folder, "sold_flag_summary.csv"), index=False)
listing_flag_summary.to_csv(os.path.join(data_folder, "listing_flag_summary.csv"), index=False)

print("\nWeek 4-5 cleaning outputs saved successfully.")