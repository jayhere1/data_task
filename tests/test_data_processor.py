"""
Pytest tests
"""

import re
from unittest.mock import MagicMock, patch

import pytest

from emis_task.main import extract_data_from_entry, format_address, safe_insert


def test_extract_data_patient(patient_entry):
    """Test extraction of data from a patient entry."""
    result = extract_data_from_entry(patient_entry)
    assert result is not None
    assert result["type"] == "Patient"
    assert result["data"]["ID"] == "1"
    assert result["data"]["Name"] == "Doe, John"


def test_extract_data_encounter(encounter_entry):
    """Test extraction of data from an encounter entry."""
    result = extract_data_from_entry(encounter_entry)
    assert result is not None
    assert result["type"] == "Encounter"
    assert result["data"]["Encounter ID"] == "E1"


def test_database_inserts(setup_test_database):
    """Test database insert operations."""
    _, session_local, patients, _ = setup_test_database
    # Create a new session
    session = session_local()
    try:
        # Begin a transaction
        session.begin()
        # Insert data into the patients table using the session
        session.execute(
            patients.insert(),
            {
                "ID": "1",
                "Name": "Doe, John",
                "Gender": "male",
                "Birth Date": "1980-01-01",
                "Deceased DateTime": None,
                "Marital Status": "single",
                "Address": "1234 Elm Street",
            },
        )
        session.commit()  # Commit the insert transaction

        # Fetch the inserted data
        result = session.execute(patients.select()).fetchone()
        session.commit()  # Commit after select, to close the transaction block

        # Access fields using column descriptors
        assert result[0] == "1", "The ID should be '1'"
        assert result[1] == "Doe, John", "The Name should be 'Doe, John'"
        assert result[2] == "male", "The Gender should be 'male'"
        assert result[3] == "1980-01-01", "The Birth Date should be '1980-01-01'"
        assert result[4] == "single", "The Marital Status should be 'single'"
        assert result[5] == "1234 Elm Street", "The Address should be '1234 Elm Street'"

    except Exception as e:
        session.rollback()  # Ensure rollback on exception
        assert False, f"An error occurred: {e}"
    finally:
        session.close()  # Always close the session


def test_extract_data_missing_fields(patient_entry):
    """Test data extraction when some optional fields are missing."""
    # Simulate missing 'address' and 'maritalStatus'
    del patient_entry["resource"]["address"]
    patient = extract_data_from_entry(patient_entry)
    assert (
        patient["data"].get("address") is None
    ), "Address should be empty if not provided"
    assert (
        patient["data"].get("Marital Status") is None
    ), "Marital Status should be None if not provided"


def test_concurrent_database_access(setup_test_database):
    """Test concurrent writes to the database to ensure data integrity."""
    _, session_local, patients, _ = setup_test_database
    # Create a new session
    session = session_local()
    session.begin()  # Mock concurrent database access
    session.execute(
        patients.insert(),
        {
            "ID": "1",
            "Name": "Doe, John",
            "Gender": "male",
            "Birth Date": "1980-01-01",
            "Deceased DateTime": None,
            "Marital Status": "single",
            "Address": "1234 Elm Street",
        },
    )
    session.commit()
    # Simulate a concurrent access trying to insert the same primary key
    with pytest.raises(Exception) as excinfo:
        session.execute(patients.insert(), {"ID": "1", "Name": "Jane Doe"})
    assert "UNIQUE constraint failed: patients.ID" in str(excinfo.value)


def test_data_format_validation(patient_entry):
    """
    Ensure that extracted data follows
    expected formats, especially for dates.
    """
    patient = extract_data_from_entry(patient_entry)
    assert re.match(
        r"\d{4}-\d{2}-\d{2}", patient["data"]["Birth Date"]
    ), "Birth Date should be in YYYY-MM-DD format"


@pytest.mark.parametrize(
    "input_address, expected_output",
    [
        (
            {
                "line": ["1234 Main St"],
                "city": "Townsville",
                "state": "TS",
                "country": "Countryland",
            },
            "1234 Main St, Townsville, TS, Countryland",
        ),
        (
            {"line": ["1234 Main St"], "city": "Townsville"},
            "1234 Main St, Townsville",
        ),
        ({}, None),
        (
            {"line": ["1234 Main St"], "state": "TS", "country": "Countryland"},
            "1234 Main St, , TS, Countryland",
        ),
        ({"city": "Townsville", "state": "TS"}, "Townsville, TS"),
    ],
)
def test_format_address(input_address, expected_output):
    """Test the address formatting from various inputs."""
    result = format_address(input_address)
    assert (
        result == expected_output
    ), f"Expected formatted address to be '{expected_output}', but got '{result}'"


def test_safe_insert_real_table(mock_table):
    """Test the safe_insert function using a real table definition."""
    conn = MagicMock()
    records = [{"ID": 1, "Name": "John Doe"}, {"ID": 2, "Name": "Jane Doe"}]

    # This time using a real table object but mocking the execute method
    conn.execute = MagicMock()

    with patch("logging.error") as mock_logging:
        safe_insert(conn, mock_table, records, "ID")
        assert conn.execute.called
        mock_logging.assert_not_called()
