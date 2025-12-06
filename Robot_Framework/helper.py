import pandas as pd
import pyarrow.parquet as pq
import os
from datetime import datetime

MIN_DATE = datetime(2025, 10, 29)

def extract_plotly_table(driver):
    columns = driver.find_elements("css selector", "g.y-column")
    if len(columns) < 3:
        raise Exception(f"Expected 3 columns, found {len(columns)}")
        
    col1 = _extract_column(columns[0])
    col2 = _extract_column(columns[1])
    col3 = _extract_column(columns[2])

    df = pd.DataFrame(
        list(zip(col1, col2, col3)),
        columns=["Facility Type", "Visit Date", "Average Time Spent"]
    )
    
    df['Visit Date Temp'] = pd.to_datetime(df["Visit Date"], errors='coerce')
    
    df = df.dropna(subset=['Visit Date Temp'])
    df = df.drop(columns=['Visit Date Temp'])

    df["Visit Date"] = pd.to_datetime(df["Visit Date"]).dt.strftime("%Y-%m-%d")
    df["Average Time Spent"] = pd.to_numeric(df["Average Time Spent"]).round(2)
    df["Facility Type"] = df["Facility Type"].astype(str)
    
    return df

def _extract_column(column_element):
    cells = column_element.find_elements("css selector", "text.cell-text")
    return [c.text.strip() for c in cells]

def read_parquet_table(folder_path):
    dfs = []
    for root, dirs, files in os.walk(folder_path):
        for f in files:
            if f.endswith(".parquet"):
                df = pq.read_table(os.path.join(root, f)).to_pandas()
                dfs.append(df)

    if not dfs:
        raise ValueError("No parquet files found")

    df = pd.concat(dfs, ignore_index=True)

    df['visit_date_dt'] = pd.to_datetime(df['visit_date'])
    df = df[df['visit_date_dt'] >= MIN_DATE]
    
    df["visit_date"] = df['visit_date_dt'].dt.strftime("%Y-%m-%d")
    df = df.drop(columns=['visit_date_dt']) 
    
    df["avg_time_spent"] = pd.to_numeric(df["avg_time_spent"]).round(2)
    
    df = df.rename(columns={
        "facility_type": "Facility Type",
        "visit_date": "Visit Date",
        "avg_time_spent": "Average Time Spent"
    })

    df = df[["Facility Type", "Visit Date", "Average Time Spent"]]

    return df

def compare_tables(df1, df2):
    cols = ["Facility Type", "Visit Date", "Average Time Spent"]
    df1s = df1[cols].sort_values(cols).reset_index(drop=True)
    df2s = df2[cols].sort_values(cols).reset_index(drop=True)
    
    if df1s.equals(df2s):
        return None

    merged = df1s.merge(df2s, on=cols, how='outer', indicator=True)
    
    diff_df1 = merged[merged['_merge'] == 'left_only'].drop(columns='_merge')
    diff_df2 = merged[merged['_merge'] == 'right_only'].drop(columns='_merge')
    
    return f"--- HTML Only ---\n{diff_df1.to_string(index=False)}\n\n--- Parquet Only ---\n{diff_df2.to_string(index=False)}"