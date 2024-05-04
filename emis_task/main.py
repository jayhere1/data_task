import pandas as pd
import json
import os

with open(
    "emis_task/exa-data-eng-assessment/data/Aaron697_Dickens475_8c95253e-8ee8-9ae8-6d40-021d702dc78e.json"
) as file:
    data = json.load(file)


patients = []
encounters = []

# Loop through each entry in the JSON data
for entry in data["entry"]:
    resource = entry["resource"]

    # Process patient resources
    if resource["resourceType"] == "Patient":
        patient = {
            "ID": resource["id"],
            "Name": f"{resource['name'][0]['family']}, {' '.join(resource['name'][0]['given'])}",
            "Gender": resource["gender"],
            "Birth Date": resource["birthDate"],
            "Deceased DateTime": resource.get("deceasedDateTime", None),
            "Marital Status": resource["maritalStatus"]["text"]
            if "maritalStatus" in resource
            else None,
        }

        # Address information
        if "address" in resource:
            patient["Address"] = {
                "Line": resource["address"][0]["line"],
                "City": resource["address"][0]["city"],
                "State": resource["address"][0]["state"],
                "Country": resource["address"][0]["country"],
            }

        # Extensions
        patient["Extensions"] = {}
        for extension in resource["extension"]:
            if "valueString" in extension:
                patient["Extensions"][extension["url"]] = extension["valueString"]
            elif "valueDecimal" in extension:
                patient["Extensions"][extension["url"]] = extension["valueDecimal"]
            elif "valueCode" in extension:
                patient["Extensions"][extension["url"]] = extension["valueCode"]
            elif "valueAddress" in extension:
                patient["Extensions"][extension["url"]] = {
                    "City": extension["valueAddress"]["city"],
                    "State": extension["valueAddress"]["state"],
                    "Country": extension["valueAddress"]["country"],
                }

        # Add the patient information to the list
        patients.append(patient)
    elif resource["resourceType"] == "Encounter":
        encounter_id = resource["id"]
        status = resource["status"]
        encounter_type = (
            resource["type"][0]["text"] if "type" in resource else "Unknown"
        )
        patient_ref = resource["subject"]["reference"]
        encounters.append(
            {
                "Encounter ID": encounter_id,
                "Status": status,
                "Type": encounter_type,
                "Patient ID": patient_ref.split(":")[-1],
            }
        )
# Print the list of patients
# for patient in patients:
#     print(patient)
# for encounter in encounters:
#     print(encounter)
