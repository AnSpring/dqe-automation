import pandas as pd


class DataQualityLibrary:
    """
    A library of static methods for performing data quality checks on pandas DataFrames.

    This class is intended to be used in a PyTest-based testing framework to validate
    the quality of data in DataFrames. Each method performs a specific data quality
    check and uses assertions to ensure that the data meets the expected conditions.
    """

    @staticmethod
    def check_duplicates(df: pd.DataFrame, column_names=None):
        if column_names:
            dup = df[df.duplicated(subset=column_names, keep=False)]
        else:
            dup = df[df.duplicated(keep=False)]

        assert dup.empty, (
            f"Found duplicate rows:\n{dup.to_string(index=False)}"
        )

    @staticmethod
    def check_count(source_df: pd.DataFrame, target_df: pd.DataFrame):
        assert len(source_df) == len(target_df), (
            f"Row count mismatch:\n"
            f"Source = {len(source_df)}, Target = {len(target_df)}"
        )

    @staticmethod
    def check_data_full_data_set(source_df: pd.DataFrame, target_df: pd.DataFrame):
        """
        Ensures every row from source_df exists in target_df.
        Allows target_df to contain more columns or additional computed attributes.
        """
        # Bring both dfs to common columns for comparison
        shared_cols = list(set(source_df.columns) & set(target_df.columns))

        missing = pd.merge(
            source_df[shared_cols],
            target_df[shared_cols],
            how="left",
            indicator=True
        ).query('_merge == "left_only"')

        assert missing.empty, (
            "Missing rows found in target dataset:\n"
            f"{missing.to_string(index=False)}"
        )

    @staticmethod
    def check_dataset_is_not_empty(df: pd.DataFrame):
        assert not df.empty, "Dataset is empty — expected at least 1 row."

    @staticmethod
    def check_not_null_values(df: pd.DataFrame, column_names):
        for col in column_names:
            assert col in df.columns, f"Column '{col}' does not exist in the dataset."
            null_count = df[col].isnull().sum()
            assert null_count == 0, f"Column '{col}' contains NULL values: {null_count} rows."