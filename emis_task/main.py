import json
import os
import logging
from multiprocessing import Pool
import sqlalchemy as db
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def extract_data_from_entry(entry):
    """
    Extract relevant patient or encounter information from a single JSON entry.
    """
    try:
        resource = entry["resource"]
        if resource["resourceType"] == "Patient":
            patient = {
                "ID": resource["id"],
                "Name": f"{resource['name'][0]['family']}, {' '.join(resource['name'][0]['given'])}",
                "Gender": resource["gender"],
                "Birth Date": resource["birthDate"],
                "Deceased DateTime": resource.get("deceasedDateTime", None),
                "Marital Status": resource.get("maritalStatus", {}).get("text", None),
            }
            if "address" in resource:
                patient["Address"] = (
                    " ".join(resource["address"][0].get("line", []))
                    + ", "
                    + resource["address"][0].get("city", "")
                    + ", "
                    + resource["address"][0].get("state", "")
                    + ", "
                    + resource["address"][0].get("country", "")
                )
            return {"type": "Patient", "data": patient}
        elif resource["resourceType"] == "Encounter":
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


def process_file(file_path):
    """
    Process a JSON file to extract patient and encounter data.
    """
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
        patients = []
        encounters = []
        for entry in data.get("entry", []):
            result = extract_data_from_entry(entry)
            if result:
                if result["type"] == "Patient":
                    patients.append(result["data"])
                elif result["type"] == "Encounter":
                    encounters.append(result["data"])
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


def main(directory):
    """
    Main execution function to process all JSON files in a directory and store the data into a database.

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
                    conn.execute(patients_table.insert(), result["patients"])
                    conn.execute(encounters_table.insert(), result["encounters"])
                except SQLAlchemyError as e:
                    logging.error(f"Database error: {e}")
        logging.info(
            f"Processed batch {i // batch_size + 1}/{(len(files) // batch_size) + 1}"
        )


if __name__ == "__main__":
    main(os.getenv("DIRECTORY_PATH", "emis_task/exa-data-eng-assessment/data/"))
