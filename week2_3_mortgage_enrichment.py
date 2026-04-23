import pandas as pd
import os

data_folder = "/Users/soniczhang/Desktop/IDX Exchange/MLS code"

sold_path = os.path.join(data_folder, "sold_validated.csv")
listing_path = os.path.join(data_folder, "listing_validated.csv")

sold = pd.read_csv(sold_path)
listings = pd.read_csv(listing_path)

# -----------------------------
# Step 1: Fetch mortgage rate data
# -----------------------------
url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=MORTGAGE30US"

# 先不要 parse_dates，先直接读
mortgage = pd.read_csv(url)

print("\nMortgage columns:")
print(mortgage.columns.tolist())

print("\nMortgage preview:")
print(mortgage.head())

# 假设前两列分别是日期和利率，直接重命名
mortgage.columns = ["date", "rate_30yr_fixed"]

# 再把 date 转成 datetime
mortgage["date"] = pd.to_datetime(mortgage["date"], errors="coerce")

# 把利率列转成 numeric，防止有 "." 或其他字符
mortgage["rate_30yr_fixed"] = pd.to_numeric(mortgage["rate_30yr_fixed"], errors="coerce")

# -----------------------------
# Step 2: Convert weekly to monthly average
# -----------------------------
mortgage["year_month"] = mortgage["date"].dt.to_period("M")
mortgage_monthly = (
    mortgage.groupby("year_month")["rate_30yr_fixed"]
    .mean()
    .reset_index()
)

print("\nMortgage monthly preview:")
print(mortgage_monthly.head())

# -----------------------------
# Step 3: Create join keys
# -----------------------------
sold["CloseDate"] = pd.to_datetime(sold["CloseDate"], errors="coerce")
sold["year_month"] = sold["CloseDate"].dt.to_period("M")

listings["ListingContractDate"] = pd.to_datetime(
    listings["ListingContractDate"], errors="coerce"
)
listings["year_month"] = listings["ListingContractDate"].dt.to_period("M")

# -----------------------------
# Step 4: Merge
# -----------------------------
sold_with_rates = sold.merge(mortgage_monthly, on="year_month", how="left")
listings_with_rates = listings.merge(mortgage_monthly, on="year_month", how="left")

# -----------------------------
# Step 5: Validate merge
# -----------------------------
print("\nNull mortgage rates in sold dataset:")
print(sold_with_rates["rate_30yr_fixed"].isnull().sum())

print("\nNull mortgage rates in listings dataset:")
print(listings_with_rates["rate_30yr_fixed"].isnull().sum())

print("\nSold preview:")
preview_cols = [col for col in ["CloseDate", "year_month", "ClosePrice", "rate_30yr_fixed"] if col in sold_with_rates.columns]
print(sold_with_rates[preview_cols].head())

# Save outputs
sold_with_rates.to_csv(os.path.join(data_folder, "sold_with_mortgage_rates.csv"), index=False)
listings_with_rates.to_csv(os.path.join(data_folder, "listing_with_mortgage_rates.csv"), index=False)

print("\nMortgage enrichment files saved successfully.")