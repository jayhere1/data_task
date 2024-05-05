import json
import os
import logging
from multiprocessing import Pool
import sqlalchemy as db
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from sqlalchemy.dialects.sqlite import insert

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def safe_insert(conn, table, records, unique_column):
    """
    Safely insert records into the specified table, ignoring duplicate
    s based on the specified unique column.

    Args:
    - conn: Database connection object from SQLAlchemy.
    - table: SQLAlchemy Table object to insert records into.
    - records: List of dictionaries, each representing a record to be inserted.
    - unique_column: The column name that has a UNIQUE constraint.
    """
    if not records:
        return  # No records to process

    for record in records:
        stmt = insert(table).values(**record)
        do_nothing_stmt = stmt.on_conflict_do_nothing(index_elements=[unique_column])
        try:
            conn.execute(do_nothing_stmt)
        except SQLAlchemyError as e:
            logging.error(f"Failed to insert record {record}: {e}")


def extract_data_from_entry(entry):
    """
    Extract relevant patient or encounter information from a single JSON entry.
    """
    try:
        resource = entry["resource"]
        resource_type = resource["resourceType"]
        if resource_type == "Patient":
            patient = {
                "ID": resource["id"],
                "Name": f"{resource['name'][0]['family']}, {' '.join(resource['name'][0]['given'])}",
                "Gender": resource["gender"],
                "Birth Date": resource["birthDate"],
                "Deceased DateTime": resource.get("deceasedDateTime"),
                "Marital Status": resource.get("maritalStatus", {}).get("text"),
                "Address": format_address(resource.get("address", [{}])[0]),
            }
            return {"type": "Patient", "data": patient}
        elif resource_type == "Encounter":
            encounter = {
                "Encounter ID": resource["id"],
                "Status": resource["status"],
                "Type": resource.get("type", [{}])[0].get("text", "Unknown"),
                "Patient ID": resource["subject"]["reference"].split(":")[-1],
            }
            return {"type": "Encounter", "data": encounter}
    except KeyError as e:
        logging.error(f"Missing key {e} in JSON entry: {entry}")
    return None


def format_address(address):
    """Helper function to format address field."""
    if not address:
        return None

    # Construct the address string from components,
    # handling missing parts
    line = ", ".join(address.get("line", []))
    city = address.get("city", "")
    state = address.get("state", "")
    country = address.get("country", "")

    formatted_address = f"{line}, {city}, {state}, {country}"
    return formatted_address.strip(", ")


def process_file(file_path):
    """
    Process a JSON file to extract patient and encounter data.
    """
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
        patients = [
            extract_data_from_entry(entry)["data"]
            for entry in data.get("entry", [])
            if extract_data_from_entry(entry)
            and extract_data_from_entry(entry)["type"] == "Patient"
        ]
        encounters = [
            extract_data_from_entry(entry)["data"]
            for entry in data.get("entry", [])
            if extract_data_from_entry(entry)
            and extract_data_from_entry(entry)["type"] == "Encounter"
        ]
        return {
            "file": file_path,
            "patients": patients,
            "encounters": encounters,
            "error": None,
        }
    except (IOError, json.JSONDecodeError) as e:
        logging.error(f"Error processing file {file_path}: {e}")
        return {"file": file_path, "error": str(e)}


def setup_database():
    """
    Setup database connection and define schema for patients and encounters.

    Returns:
    - tuple: A tuple containing the database engine and table objects for patients and encounters.
    """
    engine = db.create_engine(os.getenv("DATABASE_URL", "sqlite:///processed_data.db"))
    metadata = db.MetaData()
    patients = db.Table(
        "patients",
        metadata,
        db.Column("ID", db.String, primary_key=True),
        db.Column("Name", db.String),
        db.Column("Gender", db.String),
        db.Column("Birth Date", db.String),
        db.Column("Deceased DateTime", db.String),
        db.Column("Marital Status", db.String),
        db.Column("Address", db.String),
    )
    encounters = db.Table(
        "encounters",
        metadata,
        db.Column("Encounter ID", db.String, primary_key=True),
        db.Column("Status", db.String),
        db.Column("Type", db.String),
        db.Column("Patient ID", db.String, db.ForeignKey("patients.ID")),
    )
    metadata.create_all(engine)
    return engine, patients, encounters


def process_directory(directory):
    """
    Main function to process all JSON files in a
    directory and store the data into a database.
    Args:
    - directory (str): The directory containing JSON files.
    """
    engine, patients_table, encounters_table = setup_database()
    files = [
        os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".json")
    ]
    batch_size = 1000
    for i in range(0, len(files), batch_size):
        batch_files = files[i : i + batch_size]
        with Pool(processes=4) as pool:
            batch_results = pool.map(process_file, batch_files)
        for result in batch_results:
            if result["error"]:
                continue
            with engine.begin() as conn:
                try:
                    safe_insert(conn, patients_table, result["patients"], "ID")
                    safe_insert(
                        conn, encounters_table, result["encounters"], "Encounter ID"
                    )
                except SQLAlchemyError as e:
                    logging.error(f"Database error: {e}")
        logging.info(
            f"Processed batch {i // batch_size + 1}/{(len(files) // batch_size) + 1}"
        )


if __name__ == "__main__":
    process_directory(
        os.getenv("DIRECTORY_PATH", "emis_task/exa-data-eng-assessment/data/")
    )
