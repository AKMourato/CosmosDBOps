"""
Database Operations Toolset
"""
import os
import datetime
import json
from typing import Union
import pymongo
import pymongo_inmemory
import yaml
from bson import ObjectId
from main.utilities.utils import (
    calculate_bmi,
    calculate_mosteller_bsa,
    add_test_data_to_db,
    json_printer,
    fcsv2list,
)


class DBOps:
    """
    DBOps contains several tools to access, retrieve, modify and upload information
    to a azure cosmosdb nosql database.
    """

    def __init__(self, db: str) -> None:
        """
        Initialize the desired database.

        Args
        ------
            db: Database - development ('dev'), deployment ('deploy'), in-memory ('inmemory')

        """
        root_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
        with open(os.path.join(root_dir, "config.yaml"), encoding="utf-8") as yml:
            cfg = yaml.full_load(yml)
        if db == "dev":
            client = pymongo.MongoClient(cfg["cosmosdb"]["dev2"]["conn_str"])
            self.database = client[cfg["database"]]
        elif db == "deploy":
            client = pymongo.MongoClient(cfg["cosmosdb"]["dep"]["conn_str"])
            self.database = client[cfg["database"]]
        elif db == "inmemory":
            client = pymongo_inmemory.MongoClient()
            self.database = client.testdb
            add_test_data_to_db(self.database, cfg["collections"])
        else:
            raise ValueError("DB not supported.")

    def get_patient_collection(
        self, internal_id: int, series: Union[str, list] = None
    ) -> list:
        """
        Function to retrieve the patient collection document(s) for a specific patient.
        If series name not specified, the function will return the only patient entry
        for that human_id.

        Args
        ------
            internal_id:  Human internal ID
            series: Series name

        Return
        ------
            List of patient collection document(s)
        """

        def to_query(internal_id, series):
            if series:
                query = list(
                    self.database.patient.find(
                        {
                            "internal_info": {
                                "internal_id": internal_id,
                                "series": series,
                            }
                        }
                    )
                )
                if len(query) > 1:
                    json_printer(query)
                    raise SystemExit("More than one patient entry found.")
                return query

            query = list(
                self.database.patient.find({"internal_info.internal_id": internal_id})
            )
            if len(query) > 1:
                json_printer(query)
                raise SystemExit("More than one patient entry found.")
            return query

        if isinstance(series, list):
            query = to_query(internal_id, series)
            if query == []:
                query = to_query(internal_id, [series[1], series[0]])
            return query

        return to_query(internal_id, series)

    def get_patient_imaging_collection(
        self, internal_id: int, series: Union[str, list] = None
    ) -> dict:
        """
        Function to retrieve a patient's imaging collection document for a specific patient.

        Args
        ------
            internal_id:  Human internal ID
            series: Series name

        Return
        ------
            Patient's imaging collection document
        """
        imaging_id = self.get_patient_collection(internal_id, series)[0]["imaging_data"]
        return self.database.imaging.find_one({"_id": imaging_id})

    def get_patient_model_collection(
        self, internal_id: int, series: Union[str, list] = None
    ) -> dict:
        """
        Function to retrieve a patient's models collection document for a specific patient.

        Args
        ------
            internal_id:  Human internal ID
            series: Series name

        Return
        ------
            Patient's imaging collection document
        """

        models_id = self.get_patient_collection(internal_id, series)[0]["models"]
        return self.database.models.find_one({"_id": ObjectId(models_id)})

    def get_patient_cohort(self, **kwargs) -> Union[list, dict]:
        """
        Function to retrieve a patient cohort document.

        Kwargs
        ------
            cohort_name(str): Patient cohort name
            cohort_id(str): Patient cohort object ID

        Return
        ------
            Patient cohort document/list
        """
        cohort_name, cohort_id = [kwargs.get("cohort_name"), kwargs.get("cohort_id")]
        if cohort_name is not None and cohort_id is not None:
            raise SystemExit("Provide either the cohort name or the cohort id.")
        if cohort_name:
            return list(
                self.database["patient-cohort"].find({"cohort_name": cohort_name})
            )
        if cohort_id:
            return self.database["patient-cohort"].find_one(
                {"_id": ObjectId(cohort_id)}
            )
        return None

    def add_patients_to_cohort(self, cohort_id: str, patient_ids: list) -> list:
        """
        Function to add patients to a specific patient cohort.

        Args
        ------
            cohort_id:  Patient cohort objectID
            patient_ids: ObjectIDs of patients' collection documents

        Return
        ------
            Cohort patients ObjectIDs
        """

        self.database["patient-cohort"].update_one(
            {"_id": ObjectId(cohort_id)},
            {"$addToSet": {"patient_ids": {"$each": patient_ids}}},
        )
        self.database["patient-cohort"].update_one(
            {"_id": ObjectId(cohort_id)},
            {
                "$set": {
                    "number_patients": len(
                        self.database["patient-cohort"].find_one(
                            {"_id": ObjectId(cohort_id)}, {"patient_ids"}
                        )["patient_ids"]
                    )
                }
            },
        )

        self._update_max_min_patient_dimensions_in_cohort(cohort_id, patient_ids)

        return self.database["patient-cohort"].find_one(
            {"_id": ObjectId(cohort_id)}, {"patient_ids"}
        )["patient_ids"]

    def _update_max_min_patient_dimensions_in_cohort(
        self, cohort_id: str, patient_ids: list
    ) -> None:

        cohort_doc = self.database["patient-cohort"].find_one(
            {"_id": ObjectId(cohort_id)}, {"weight", "height"}
        )
        for id_ in patient_ids:
            patient_doc = self.database.patient.find_one(
                {"_id": ObjectId(id_)}, {"weight", "height"}
            )
            for i in ["height", "weight"]:
                if (
                    cohort_doc[i]["min"] is not None
                    and patient_doc[i] is not None
                    and patient_doc[i] < cohort_doc[i]["min"]
                ):
                    cohort_doc[i]["min"] = patient_doc[i]
                elif (
                    cohort_doc[i]["max"] is not None
                    and patient_doc[i] is not None
                    and patient_doc[i] > cohort_doc[i]["max"]
                ):
                    cohort_doc[i]["max"] = patient_doc[i]
                elif (
                    cohort_doc[i]["min"] is None
                    and cohort_doc[i]["max"] is None
                    and patient_doc[i] is not None
                ):
                    cohort_doc[i]["min"] = patient_doc[i]
                    cohort_doc[i]["max"] = patient_doc[i]

        self.database["patient-cohort"].update_one(
            {"_id": ObjectId(cohort_id)},
            {
                "$set": {
                    "height.min": cohort_doc["height"]["min"],
                    "height.max": cohort_doc["height"]["max"],
                    "weight.min": cohort_doc["weight"]["min"],
                    "weight.max": cohort_doc["weight"]["max"],
                }
            },
        )

    def set_max_min_patient_dimensions_in_cohort(self, cohort_id: str) -> dict:
        """
        Function to set the max and min patients' dimensions in a patient cohort.

        Args
        ------
            cohort_id:  Patient cohort objectID

        Return
        ------
            Patient cohort document
        """

        cohort_doc = self.database["patient-cohort"].find_one(
            {"_id": ObjectId(cohort_id)}, {"weight", "height", "patient_ids"}
        )
        for id_ in cohort_doc["patient_ids"]:
            patient_doc = self.database.patient.find_one(
                {"_id": ObjectId(id_)}, {"weight", "height"}
            )
            for i in ["height", "weight"]:
                if (
                    cohort_doc[i]["min"] is not None
                    and patient_doc[i] is not None
                    and patient_doc[i] < cohort_doc[i]["min"]
                ):
                    cohort_doc[i]["min"] = patient_doc[i]
                elif (
                    cohort_doc[i]["max"] is not None
                    and patient_doc[i] is not None
                    and patient_doc[i] > cohort_doc[i]["max"]
                ):
                    cohort_doc[i]["max"] = patient_doc[i]
                elif (
                    cohort_doc[i]["min"] is None
                    and cohort_doc[i]["max"] is None
                    and patient_doc[i] is not None
                ):
                    cohort_doc[i]["min"] = patient_doc[i]
                    cohort_doc[i]["max"] = patient_doc[i]

        self.database["patient-cohort"].update_one(
            {"_id": ObjectId(cohort_id)},
            {
                "$set": {
                    "height.min": cohort_doc["height"]["min"],
                    "height.max": cohort_doc["height"]["max"],
                    "weight.min": cohort_doc["weight"]["min"],
                    "weight.max": cohort_doc["weight"]["max"],
                }
            },
        )

        return self.database["patient-cohort"].find_one({"_id": ObjectId(cohort_id)})

    def update_human_demographics(self, internal_id: int, **kwargs) -> None:
        # pylint: disable=too-many-branches
        """
        Function to update a patients' demographic info.

        Args
        ------
            internal_id:  Human internal ID

        Kwargs
        ------
            age (int|float) : Human age
            gender (str) : Human gender
            height (int|float) : Human height [cm]
            weight (int|float) : Human weight [kg]
            loc (str) : Human origin_location
        """

        age, gender, height, weight, origin_location = [
            kwargs.get("age"),
            kwargs.get("gender"),
            kwargs.get("height"),
            kwargs.get("weight"),
            kwargs.get("loc"),
        ]
        if not isinstance(age, (int, float)) and age is not None:
            raise ValueError("Age must be an integer or float.")
        if age not in range(0, 121) and age is not None:
            raise ValueError("Age not inside the valid range.")
        if age:
            self.database.patient.update_many(
                {"internal_info.internal_id": internal_id}, {"$set": {"age": age}}
            )
        if gender not in ["male", "female"] and gender is not None:
            raise ValueError("Gender not male/female.")
        if gender:
            self.database.patient.update_many(
                {"internal_info.internal_id": internal_id}, {"$set": {"gender": gender}}
            )
        for i in [height, weight]:
            if not isinstance(i, (int, float)) and i is not None:
                raise ValueError("Height/weight must be an integer or float.")
        if height:
            self.database.patient.update_many(
                {"internal_info.internal_id": internal_id}, {"$set": {"height": height}}
            )
            check_weight = self.database.patient.find_one(
                {"internal_info.internal_id": internal_id}, {"weight"}
            )["weight"]
            if check_weight is not None:
                bmi = calculate_bmi(check_weight, height)
                bsa = calculate_mosteller_bsa(check_weight, height)
                self.database.patient.update_many(
                    {"internal_info.internal_id": internal_id}, {"$set": {"bmi": bmi}}
                )
                self.database.patient.update_many(
                    {"internal_info.internal_id": internal_id}, {"$set": {"bsa": bsa}}
                )
        if weight:
            self.database.patient.update_many(
                {"internal_info.internal_id": internal_id}, {"$set": {"weight": weight}}
            )
            check_height = self.database.patient.find_one(
                {"internal_info.internal_id": internal_id}, {"height"}
            )["height"]
            if check_height is not None:
                bmi = calculate_bmi(weight, check_height)
                bsa = calculate_mosteller_bsa(weight, check_height)
                self.database.patient.update_many(
                    {"internal_info.internal_id": internal_id}, {"$set": {"bmi": bmi}}
                )
                self.database.patient.update_many(
                    {"internal_info.internal_id": internal_id}, {"$set": {"bsa": bsa}}
                )

        if (
            origin_location
            not in [
                "europe",
                "asia_pacific",
                "north_south_america",
                "middle_east_africa",
            ]
            and origin_location is not None
        ):
            raise ValueError("Origin location not in the accepted values/format.")
        if origin_location:
            self.database.patient.update_many(
                {"internal_info.internal_id": internal_id},
                {"$set": {"origin_location": origin_location}},
            )

    def get_patient_model_list(
        self, internal_id: int, series: Union[str, list] = None
    ) -> list:
        """
        Function to retrieve a patient models list from its model collection document.

        Args
        ------
            internal_id:  Human internal ID
            series: Series name

        Return
        ------
            Patient model list
        """
        return self.get_patient_model_collection(internal_id, series)["models"]

    def update_patient_model_list(
        self, internal_id: int, models: list, series: Union[str, list] = None
    ) -> dict:
        """
        Function to update a patient models list of its model collection document.

        Args
        ------
            internal_id:  Human internal ID
            series: Series name
            models: Models list

        Return
        ------
            Patient's models collection document
        """

        query = self.get_patient_collection(internal_id, series)[0]
        self.database.models.find_one_and_update(
            {"_id": ObjectId(query["models"])}, {"$set": {"models": models}}
        )
        return self.database.models.find_one({"_id": ObjectId(query["models"])})

    def append_blobs_to_submodel(
        self,
        internal_id: int,
        timestamp: Union[int, float],
        blobs: list,
        series: Union[str, list] = None,
    ) -> dict:
        """
        Function to append submodel blobs to an existing model timestamp.

        Args
        ------
            internal_id:  Human internal ID
            series: Series name
            timestamp: heart cycle phase (frame)
            blob: submodel blob

        Return
        ------
            Patient's models collection document
        """

        query = self.get_patient_collection(internal_id, series)
        models_id = ObjectId(query[0]["models"])
        model_dict = self.database.models.find_one({"_id": models_id})
        for idx, i in enumerate(model_dict["models"]):
            if i["timestamp"] == timestamp:
                model_dict["models"][idx]["sub_models"].extend(blobs)
        self.database.models.find_one_and_update(
            {"_id": models_id}, {"$set": {"models": model_dict["models"]}}
        )
        return self.database.models.find_one({"_id": models_id})

    # pylint: disable=too-many-arguments
    def append_landmarks_to_model(
        self,
        internal_id: int,
        timestamp: Union[int, float],
        path_fcsv: str,
        name: str,
        type_of_spline: str,
        description: str,
        series: Union[str, list] = None,
    ) -> dict:
        """
        Function to append landmarks to an existing model timestamp

        Args
        ------
            internal_id: Human internal ID
            timestamp: heart cycle phase (frame)
            path_fcsv: path to fcsv with landmarks included
            name: name of the landmark points
            description: description landmark points
            series: Series name

        """

        landmarks = fcsv2list(path_fcsv)
        landmarks = {
            "name": name,
            "value": landmarks,
            "description": description,
            "type_of_spline": type_of_spline,
        }

        query = self.get_patient_collection(internal_id, series)
        models_id = ObjectId(query[0]["models"])
        model_dict = self.database.models.find_one({"_id": models_id})
        for i in model_dict["models"]:
            if i["timestamp"] == timestamp:
                i["landmarks"].append(landmarks)

        self.database.models.find_one_and_update(
            {"_id": models_id}, {"$set": {"models": model_dict["models"]}}
        )

        return self.database.models.find_one({"_id": models_id})

    def upload_patients(
        self, jsondir: str, patients_list: list = None, internal_id_list: list = None
    ) -> list:
        # pylint: disable=too-many-locals,too-many-branches
        """
        Function to upload patients to the database.
        If patients_list is set, it assumes that the JSONs root directory
        presents the following structure:

            /jsondir/
                    ├── 00122
                    │   └── SER00001
                    │       │   └── imaging_collection.json
                    |       |   └── patient_collection.json
                    |       |   └── model_collection.json
                    │   └── SER00002
                    │       │   └── (....)
                    └── 00169
                    │   └── SER00009
                    │       │   └── (....)

        If internal_id_list is set, it assumes that the JSONs root directory
        presents the following structure:

            /jsondir/
                    ├── 00122
                    │   └── imaging_collection.json
                    |   └── patient_collection.json
                    |   └── model_collection.json
                    │
                    └── 00169
                    │   └── (....)

        Args
        ------
            jsondir: JSONs root directory
            patients_list: List of patients' dict (e.g: [{122:"SER00003"}, {169:"SER00009"}, (...)])
            internal_id_list: List of human_ids (e.g: [122,169])

        Return
        ------
            pat_ids: Patients' patient collection document IDs
        """

        if patients_list is None and internal_id_list is None:
            raise SystemError("Provide patient list or internal id list.")
        if patients_list is not None and internal_id_list is not None:
            raise SystemError("Either patient list or internal id list can be defined.")

        if internal_id_list:
            patients_list = []
            for i in internal_id_list:
                human_id = f"{int(i):05}"
                if any(
                    os.path.isdir(os.path.join(jsondir, human_id, x))
                    for x in os.listdir(os.path.join(jsondir, human_id))
                ):
                    raise SystemError(
                        "Directories where found in {}. Provide patients_list.".format(
                            os.path.join(jsondir, human_id)
                        )
                    )
                with open(
                    os.path.join(jsondir, human_id, "patient_collection.json"),
                    "r",
                    encoding="utf-8",
                ) as file:
                    patient_dict = json.load(file)
                patients_list.append(
                    {int(human_id): patient_dict["internal_info"]["series"]}
                )

        for patient in patients_list:
            human = next(iter(patient))
            series = patient[human]
            if self.get_patient_collection(human, series):
                raise SystemExit(patient, "Patient already present in the database.")

            if internal_id_list:
                for i in ["patient", "model"]:
                    if not os.path.exists(
                        os.path.join(
                            jsondir,
                            f"{int(human):05}",
                            "{}_collection.json".format(i),
                        )
                    ):
                        raise FileNotFoundError(
                            os.path.join(
                                jsondir,
                                f"{int(human):05}",
                                "{}_collection.json".format(i),
                            )
                        )
            else:
                for i in ["patient", "model"]:
                    if not os.path.exists(
                        os.path.join(
                            jsondir,
                            f"{int(human):05}",
                            series,
                            "{}_collection.json".format(i),
                        )
                    ):
                        raise FileNotFoundError(
                            os.path.join(
                                jsondir,
                                f"{int(human):05}",
                                series,
                                "{}_collection.json".format(i),
                            )
                        )

        now = datetime.datetime.utcnow()
        pat_ids = []
        for patient in patients_list:
            human = next(iter(patient))
            series = patient[human]
            if not self.get_patient_collection(human, series):
                if internal_id_list:
                    with open(
                        os.path.join(
                            jsondir,
                            f"{int(human):05}",
                            "model_collection.json",
                        ),
                        encoding="utf-8",
                    ) as file:
                        model_dict = json.load(file)
                        model_dict["datetime_creation"] = now
                    model_id = self.database.models.insert_one(model_dict).inserted_id
                    with open(
                        os.path.join(
                            jsondir, f"{int(human):05}", "patient_collection.json"
                        ),
                        encoding="utf-8",
                    ) as file:
                        pat_dict = json.load(file)
                        pat_dict["datetime_creation"] = now
                else:
                    with open(
                        os.path.join(
                            jsondir, f"{int(human):05}", series, "model_collection.json"
                        ),
                        encoding="utf-8",
                    ) as file:
                        model_dict = json.load(file)
                        model_dict["datetime_creation"] = now
                    model_id = self.database.models.insert_one(model_dict).inserted_id
                    with open(
                        os.path.join(
                            jsondir,
                            f"{int(human):05}",
                            series,
                            "patient_collection.json",
                        ),
                        encoding="utf-8",
                    ) as file:
                        pat_dict = json.load(file)
                        pat_dict["datetime_creation"] = now
                pat_dict["models"] = str(model_id)
                patient_id = self.database.patient.insert_one(pat_dict).inserted_id
                pat_ids.append(str(patient_id))
            else:
                raise SystemExit("Patient already present in the database.")
        return pat_ids

    def upload_patients_add_to_cohort(
        self,
        jsondir: str,
        cohort_id: str,
        patients_list: list = None,
        internal_id_list: list = None,
    ) -> dict:
        """
        Function to upload patients to the database and add them to a cohort.
        If patients_list is set, it assumes that the JSONs root directory
        presents the following structure:

            /jsondir/
                    ├── 00122
                    │   └── SER00001
                    │       │   └── imaging_collection.json
                    |       |   └── patient_collection.json
                    |       |   └── model_collection.json
                    │   └── SER00002
                    │       │   └── (....)
                    └── 00169
                    │   └── SER00009
                    │       │   └── (....)

        If internal_id_list is set, it assumes that the JSONs root directory
        presents the following structure:

            /jsondir/
                    ├── 00122
                    │   └── imaging_collection.json
                    |   └── patient_collection.json
                    |   └── model_collection.json
                    │
                    └── 00169
                    │   └── (....)
        Args
        ------
            jsondir: JSONs root directory
            cohort_name: Name of the patient cohort to add the patients
            patients_list: List of patients' dict (e.g: [{122:"SER00003"}, {169:"SER00009"}, (...)])
            internal_id_list: List of human_ids (e.g: [122,169])

        Return
        ------
            Patient cohort
        """
        if patients_list is None and internal_id_list is None:
            raise SystemError("Provide patient list or internal id list.")
        if patients_list is not None and internal_id_list is not None:
            raise SystemError("Either patient list or internal id list can be defined.")

        if internal_id_list:
            pat_ids = self.upload_patients(jsondir, internal_id_list=internal_id_list)
        else:
            pat_ids = self.upload_patients(jsondir, patients_list=patients_list)
        self.add_patients_to_cohort(cohort_id, pat_ids)
        return self.database["patient-cohort"].find_one({"_id": ObjectId(cohort_id)})

    def get_all_patients_patientcoll(self) -> dict:
        """
        Function to retrieve all patients inside patient collection.
        """

        docs = list(self.database.patient.find())
        patients = {}
        for i in docs:
            if "internal_info" in i.keys():
                patients[i["internal_info"]["internal_id"]] = i["internal_info"][
                    "series"
                ]
        return dict(sorted(patients.items()))
