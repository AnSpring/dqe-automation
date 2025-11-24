import pandas as pd
import os
import pyarrow.parquet as pq


class ParquetReader:
    """
    Reads Parquet files from file system and returns pandas DataFrames.
    Supports reading folders and nested folders with multiple parquet files.
    """

    def process(self, path: str, include_subfolders: bool = False) -> pd.DataFrame:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Parquet path does not exist: {path}")

        files = []

        if os.path.isfile(path):
            # Direct parquet file
            if path.endswith(".parquet"):
                files.append(path)

        else:
            # Folder — walk through it
            if include_subfolders:
                for root, _, filenames in os.walk(path):
                    for f in filenames:
                        if f.endswith(".parquet"):
                            files.append(os.path.join(root, f))
            else:
                # No recursion
                files = [
                    os.path.join(path, f)
                    for f in os.listdir(path)
                    if f.endswith(".parquet")
                ]

        if not files:
            raise ValueError(f"No parquet files found in path: {path}")

        # Read all files and concat into one DataFrame
        df_list = [pq.read_table(f).to_pandas() for f in files]

        return pd.concat(df_list, ignore_index=True)