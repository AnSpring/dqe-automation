import pytest
import re


def test_file_not_empty(csv_content):
    assert len(csv_content) > 0, "CSV file is empty, data is not loaded"


@pytest.mark.validate_csv
def test_validate_schema(actual_schema, expected_schema, schema_validator):
    schema_validator(actual_schema, expected_schema)


@pytest.mark.validate_csv
@pytest.mark.skip(reason="Task requirements: age validation is skipped")
def test_age_column_valid(csv_content):
    for row in csv_content:
        age = int(row["age"])
        assert 0 <= age <= 100, f"Age {age} is out of [0, 100] range"


@pytest.mark.validate_csv
def test_email_column_valid(csv_content):
    pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    for row in csv_content:
        email = row["email"]
        assert re.match(pattern, email), f"Incorrect email: {email}"


@pytest.mark.validate_csv
@pytest.mark.xfail(reason="There are duplicates in the CSV file")
def test_duplicates(csv_content):
    seen = set()
    for row in csv_content:
        tup = tuple(row.values())
        assert tup not in seen, f"Duplicate of the row: {row}"
        seen.add(tup)


@pytest.mark.validate_csv
@pytest.mark.parametrize("player_id, expected_is_active", [(1, False), (2, True)])
def test_active_players(csv_content, player_id, expected_is_active):
    row = next(
        (r for r in csv_content if int(r["id"]) == player_id),
        None
    )
    assert row is not None, f"Player with id={player_id} not found in CSV"

    is_active = row["is_active"]
    assert isinstance(is_active, bool), f"is_active has unexpected type: {type(is_active)}"

    assert is_active == expected_is_active, (
        f"For id={player_id} expected is_active={expected_is_active}, "
        f"but got {is_active}"
    )


def test_active_player(csv_content):
    row = next(
        (r for r in csv_content if int(r["id"]) == 2),
        None
    )
    assert row is not None, "Player with id=2 not found in CSV"

    is_active = row["is_active"]
    assert isinstance(is_active, bool), f"is_active has unexpected type: {type(is_active)}"

    assert is_active is True, "Expected is_active=True for id=2"
