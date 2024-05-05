import pytest
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    ForeignKey,
    Integer,
)
from sqlalchemy.orm import sessionmaker


@pytest.fixture
def patient_entry():
    """Fixture for simulating a patient JSON entry."""
    return {
        "resource": {
            "resourceType": "Patient",
            "id": "1",
            "name": [{"family": "Doe", "given": ["John"]}],
            "gender": "male",
            "birthDate": "1990-01-01",
            "address": [
                {
                    "line": ["1234 Elm St"],
                    "city": "Somewhere",
                    "state": "CA",
                    "country": "USA",
                }
            ],
        }
    }


@pytest.fixture
def encounter_entry():
    """Fixture for simulating an encounter JSON entry."""
    return {
        "resource": {
            "resourceType": "Encounter",
            "id": "E1",
            "status": "completed",
            "subject": {"reference": "Patient:1"},
        }
    }


@pytest.fixture
def setup_test_database():
    """
    Fixture to set up a test database with SQLAlchemy, including session management,
    used for testing database interactions.
    """
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    patients = Table(
        "patients",
        metadata,
        Column("ID", String, primary_key=True),
        Column("Name", String),
        Column("Gender", String),
        Column("Birth Date", String),
        Column("Marital Status", String),
        Column("Address", String),
    )
    encounters = Table(
        "encounters",
        metadata,
        Column("Encounter ID", String, primary_key=True),
        Column("Status", String),
        Column("Type", String),
        Column("Patient ID", String, ForeignKey("patients.ID")),
    )
    metadata.create_all(engine)

    # Yield engine, session factory, and tables to the test, then close the session and dispose engine after tests are done.
    try:
        yield engine, SessionLocal, patients, encounters
    finally:
        engine.dispose()


@pytest.fixture
def mock_files(tmp_path):
    d = tmp_path / "sub"
    d.mkdir()
    (d / "file1.json").write_text(
        '{"entry": [{"resource": {"resourceType": "Patient", "id": "1", "name": {"family": "Doe", "given": ["John"]}, "gender": "male", "birthDate": "2000-01-01"}}]}'
    )
    (d / "file2.json").write_text(
        '{"entry": [{"resource": {"resourceType": "Encounter", "id": "E1", "status": "completed", "subject": {"reference": "Patient:1"}, "type": [{"text": "Outpatient"}]}}]}'
    )
    return d


@pytest.fixture
def mock_table():
    metadata = MetaData()
    return Table(
        "test_table",
        metadata,
        Column("ID", Integer, primary_key=True),
        Column("Name", String),
    )
