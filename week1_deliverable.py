import pandas as pd
import glob
import os

# 改成你存 CSV 的那个真实文件夹路径
data_folder = "/Users/soniczhang/Desktop/IDX Exchange/MLS code"

sold_files = sorted(glob.glob(os.path.join(data_folder, "CRMLSSold*.csv")))
listing_files = sorted(glob.glob(os.path.join(data_folder, "CRMLSListing*.csv")))

print("Sold files found:", sold_files)
print("Listing files found:", listing_files)

def combine_and_filter(file_list, output_file):
    if not file_list:
        print(f"No files found for {output_file}")
        return

    dfs = []
    total_rows_before_concat = 0

    print(f"\nProcessing files for {output_file} ...")

    for file in file_list:
        df = pd.read_csv(file)
        df.columns = df.columns.str.strip()

        print(f"{os.path.basename(file)} rows before concat: {len(df)}")

        total_rows_before_concat += len(df)
        dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)

    print(f"Total rows before concat: {total_rows_before_concat}")
    print(f"Rows after concat: {len(combined_df)}")

    if "PropertyType" not in combined_df.columns:
        print("Available columns are:")
        print(combined_df.columns.tolist())
        raise KeyError("Column 'PropertyType' not found.")

    print(f"Rows before Residential filter: {len(combined_df)}")

    residential_df = combined_df[
        combined_df["PropertyType"].astype(str).str.strip().str.lower() == "residential"
    ].copy()

    print(f"Rows after Residential filter: {len(residential_df)}")

    # 输出文件也直接存到同一个 data folder
    output_path = os.path.join(data_folder, output_file)
    residential_df.to_csv(output_path, index=False)
    print(f"Saved file: {output_path}")

combine_and_filter(sold_files, "combined_sold_residential.csv")
combine_and_filter(listing_files, "combined_listing_residential.csv")