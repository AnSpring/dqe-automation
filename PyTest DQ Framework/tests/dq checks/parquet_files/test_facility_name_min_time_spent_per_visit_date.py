"""
Description: Data Quality checks for facility_name_min_time_spent_per_visit_date dataset.
Requirement(s): TICKET-2222
Author(s): Anastazja Bobrowa
"""


import pytest
import pandas as pd

@pytest.fixture(scope='module')
def source_data(db_connection):
    """
    Loads source data from PostgreSQL.
    """

    source_query = """
        SELECT facility_name, visit_date, min_time_spent
        FROM core.facility_name_min_time_spent_per_visit_date
        ORDER BY facility_name, visit_date"""

    source_df = db_connection.get_data_sql(source_query)
    return source_df


@pytest.fixture(scope='module')
def target_data(parquet_reader):
    """
    Loads transformed data from Parquet.
    """

    target_path = "/parquet_data/facility_name_min_time_spent_per_visit_date"

    target_df = parquet_reader.process(
        target_path,
        include_subfolders=True
    )

    return target_df

@pytest.mark.parquet_data
@pytest.mark.smoke
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_dataset_is_not_empty(target_data, data_quality_library):
    data_quality_library.check_dataset_is_not_empty(target_data)


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_data_full_data_set(source_data, target_data, data_quality_library):
    data_quality_library.check_data_full_data_set(source_data, target_data)


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_count(source_data, target_data, data_quality_library):
    data_quality_library.check_count(source_data, target_data)


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_duplicates(target_data, data_quality_library):
    data_quality_library.check_duplicates(target_data)


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_not_null(target_data, data_quality_library):
    """
    Important columns must NOT contain NULLs.
    """

    required_columns = ["facility_name","visit_date","min_time_spent"]

    data_quality_library.check_not_null_values(target_data, required_columns)