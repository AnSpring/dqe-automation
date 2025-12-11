import pytest
import pandas as pd

@pytest.fixture(scope="session")
def path_to_file():
    return "src/data/data.csv"

# Fixture to read the CSV file
@pytest.fixture(scope="session")
def csv_content(path_to_file):
    df = pd.read_csv(path_to_file)
    return df.to_dict(orient="records")

# Fixture to validate the schema of the file
@pytest.fixture(scope="session")
def actual_schema(path_to_file):
    df = pd.read_csv(path_to_file)
    return list(df.columns)

@pytest.fixture(scope="session")
def expected_schema():
    return ["id", "name", "age", "email", "is_active"]


@pytest.fixture(scope="session")
def schema_validator():
    def _validate(actual, expected):
        assert actual == expected, (f"Schema mismatch!\n"
                                    f"Actual: {actual}\n"
                                    f"Expected: {expected}")
    return _validate

# Pytest hook to mark unmarked tests with a custom mark
def pytest_collection_modifyitems(items):
    for item in items:
        if not item.iter_markers():
            item.add_marker("unmarked")
