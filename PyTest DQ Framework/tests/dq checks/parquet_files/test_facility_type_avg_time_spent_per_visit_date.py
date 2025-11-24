"""
Description: Data Quality checks for test_facility_type_avg_time_spent_per_visit_date dataset
Requirement(s): TICKET-1111
Author(s): Anastazja Bobrowa
"""

import pytest

@pytest.fixture(scope="module")
def source_data(db_connection):
    query = """
        SELECT facility_type,visit_date, avg_time_spent
        FROM core.facility_type_avg_time_spent_per_visit_date
        ORDER BY facility_type, visit_date"""
    return db_connection.get_data_sql(query)


@pytest.fixture(scope="module")
def target_data(parquet_reader):
    target_path = "/parquet_data/facility_type_avg_time_spent_per_visit_date"
    return parquet_reader.process(target_path, include_subfolders=True)


@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
@pytest.mark.smoke
def test_check_dataset_is_not_empty(target_data, data_quality_library):
    data_quality_library.check_dataset_is_not_empty(target_data)


@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_check_count(source_data, target_data, data_quality_library):
    data_quality_library.check_count(source_data, target_data)


@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_check_data_full_data_set(source_data, target_data, data_quality_library):
    data_quality_library.check_data_full_data_set(source_data, target_data)


@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_check_uniqueness(target_data, data_quality_library):
    data_quality_library.check_duplicates(target_data)


@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_check_not_null_values(target_data, data_quality_library):
    required_columns = ["facility_type", "visit_date", "avg_time_spent"]
    data_quality_library.check_not_null_values(target_data, required_columns)