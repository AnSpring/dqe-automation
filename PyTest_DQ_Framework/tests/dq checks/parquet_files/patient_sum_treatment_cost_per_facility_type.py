"""
Description: Data Quality checks for patient_sum_treatment_cost_per_facility_type dataset
Requirement(s): TICKET-3333
Author(s): Anastazja Bobrowa
"""

import pytest


@pytest.fixture(scope="module")
def source_data(db_connection):
    """
    Load aggregated source dataset from Postgres.
    """
    query = """
    SELECT
        f.facility_type,
        v.patient_id,
        SUM(v.treatment_cost) AS total_cost
    FROM public.src_generated_visits v
    JOIN public.src_generated_facilities f 
        ON f.facility_id = v.facility_id
    GROUP BY 1,2
    ORDER BY 1,2;
"""

    try:
        return db_connection.get_data_sql(query)
    except Exception as e:
        pytest.fail(f"Failed to fetch source data: {e}")


@pytest.fixture(scope="module")
def target_data(parquet_reader):
    """
    Load target Parquet data.
    """
    target_path = "PyTest_DQ_Framework/parquet_data/patient_sum_treatment_cost_per_facility_type"

    try:
        return parquet_reader.process(target_path, include_subfolders=True)
    except Exception as e:
        pytest.fail(f"Failed to load target parquet file: {e}")


@pytest.mark.parquet_data
@pytest.mark.smoke
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_dataset_is_not_empty(target_data, data_quality_library):
    data_quality_library.check_dataset_is_not_empty(target_data)


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_data_full_data_set(source_data, target_data, data_quality_library):
    data_quality_library.check_data_full_data_set(source_data, target_data)


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_count(source_data, target_data, data_quality_library):
    data_quality_library.check_count(source_data, target_data)


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_duplicates(target_data, data_quality_library):
    data_quality_library.check_duplicates(target_data)


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_not_null_values(target_data, data_quality_library):
    required_columns = ["facility_type", "patient_id", "total_cost"]
    data_quality_library.check_not_null_values(target_data, required_columns)