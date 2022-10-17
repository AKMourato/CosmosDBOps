"""
Test units for database operations.
"""
# pylint: disable=missing-function-docstring
import os
import pytest
from main.database_ops import DBOps


class TestDBOps:
    """Test class for database_ops file."""

    db = DBOps("inmemory")
    jsondir = "tests/data/upload_data/"

    def test_db_init(self):
        assert self.db.database.client.HOST == "localhost"
        assert (
            len(
                self.db.database.list_collection_names(include_system_collections=False)
            )
            > 1
        )

    def test_get_patient_collection(self, capsys):
        query = self.db.get_patient_collection(850, "SER00002")
        query_1 = self.db.get_patient_collection(850)
        assert isinstance(query[0], dict) and isinstance(query_1[0], dict)
        assert (
            int(query[0]["age"]) == 46
            and query[0]["ed_timestamp"] == 2
            and query[0]["es_timestamp"] is None
        )
        assert (
            int(query_1[0]["age"]) == 46
            and query_1[0]["ed_timestamp"] == 2
            and query_1[0]["es_timestamp"] is None
        )

        # test interchangeability
        query_2 = self.db.get_patient_collection(740, ["SER00008", "SER00009"])
        query_3 = self.db.get_patient_collection(740, ["SER00009", "SER00008"])
        assert isinstance(query_2[0], dict) and isinstance(query_3[0], dict)
        assert int(query_2[0]["age"]) == 83 == int(query_3[0]["age"])
        assert query_2[0]["ed_timestamp"] == 0.82 == query_3[0]["ed_timestamp"]
        assert query_2[0]["es_timestamp"] == 0.4 == query_3[0]["es_timestamp"]

        with pytest.raises(SystemExit):
            self.db.get_patient_collection(843)
            out, _ = capsys.readouterr()
            assert out == "More than one patient entry found."

    def test_get_all_patients_patientcoll(self):
        query = self.db.get_all_patients_patientcoll()
        assert query == {
            663: "SER00302",
            740: ["SER00008", "SER00009"],
            843: ["SER00005", "SER00009"],
            850: "SER00002",
        }

    def test_get_patient_model_collection(self):
        query = self.db.get_patient_model_collection(850, "SER00002")
        assert isinstance(query, dict)
        assert str(query["_id"]) == "62c4169ec51848f33fa6b2a2"
        assert query["models"][0]["timestamp"] == 2

        query_2 = self.db.get_patient_model_collection(740, ["SER00008", "SER00009"])
        assert isinstance(query_2, dict)
        assert str(query_2["_id"]) == "62b445e077febb2c27a41c7d"
        assert (
            query_2["models"][0]["timestamp"] == 0.82
            and query_2["models"][1]["timestamp"] == 0.40
        )

    def test_get_patient_cohort(self):
        query = self.db.get_patient_cohort(cohort_id="626aba549ce90c7ccbe9510c")
        query_2 = self.db.get_patient_cohort(cohort_name="Pulsify ED Patients")[0]
        assert str(query["_id"]) == "626aba549ce90c7ccbe9510c" == str(query_2["_id"])
        assert (
            ["severe_heart_failure", "aortic_stenosis"]
            == query["pathology"]["include"]
            == query_2["pathology"]["include"]
        )

    def test_add_patients_to_cohort(self):
        pat850_id = "5f7f7ee40bf2b2706460424c"
        cohort_id = "626aba549ce90c7ccbe9520e"
        self.db.add_patients_to_cohort(cohort_id, [pat850_id])
        cohort_doc = self.db.get_patient_cohort(cohort_id=cohort_id)
        assert pat850_id in cohort_doc["patient_ids"]
        assert cohort_doc["number_patients"] == 1

    @pytest.mark.parametrize("series", ["SER00005", ["SER00005", "SER00009"]])
    def test_update_human_demographics(self, series):
        human_id = 843
        self.db.update_human_demographics(
            human_id,
            age=63,
            gender="male",
            height=200.0,
            weight=93.0,
            loc="north_south_america",
        )
        query = self.db.get_patient_collection(human_id, series)
        assert (
            query[0]["age"] == 63
            and query[0]["gender"] == "male"
            and query[0]["height"] == 200.0
            and query[0]["weight"] == 93.0
            and query[0]["origin_location"] == "north_south_america"
        )

    @pytest.mark.parametrize("series", ["SER00302", None])
    def test_get_patient_model_list(self, capsys, series):
        query = self.db.get_patient_model_list(663, series)
        assert (
            query[0]["timestamp"] == 0 and query[0]["sub_models"][0]["name"] == "Aorta"
        )
        assert query[-1]["timestamp"] == 0.9

        with pytest.raises(SystemExit):
            self.db.get_patient_model_list(843)
            out, _ = capsys.readouterr()
            assert out == "More than one patient entry found."

    @pytest.mark.parametrize("series", ["SER00002", None])
    def test_update_patient_model_list(self, capsys, series):
        models_list = [
            {
                "timestamp": 2,
                "sub_models": [
                    {
                        "blob": "v-patients/00850/dec_models/pat850-ser002-RA_SVC_IVC.stl",
                        "name": "Right Atrium with SVC and IVC",
                    }
                ], "landmarks": []
            }
        ]
        human_id = 850
        self.db.update_patient_model_list(human_id, models_list, series)
        query = self.db.get_patient_model_list(human_id, series)
        assert query == models_list

        with pytest.raises(SystemExit):
            self.db.update_patient_model_list(843, models_list)
            out, _ = capsys.readouterr()
            assert out == "More than one patient entry found."

    @pytest.mark.parametrize("series", ["SER00302", None])
    def test_append_blobs_to_submodel(self, capsys, series):
        timestamp = 0.9
        submodel_blobs_list = [
            {
                "blob": "v-patients/00663/dec_models/pat663-ser302-frame095-thorax-sternum.stl",
                "name": "Sternum",
            },
            {
                "blob": "v-patients/00663/dec_models/pat663-ser302-frame095-thorax-lungs.stl",
                "name": "Lungs",
            },
        ]
        human_id = 663

        existing_submodel_list = self.db.get_patient_model_list(human_id, series)[-1][
            "sub_models"
        ]
        existing_model_list_excluding_timestamp = self.db.get_patient_model_list(
            human_id, series
        )[:-1]
        self.db.append_blobs_to_submodel(
            human_id, timestamp, submodel_blobs_list, series
        )
        existing_submodel_list.extend(submodel_blobs_list)

        query = self.db.get_patient_model_list(human_id, series)
        assert query[-1]["timestamp"] == timestamp
        assert query[:-1] == existing_model_list_excluding_timestamp
        assert query[-1]["sub_models"] == existing_submodel_list

        with pytest.raises(SystemExit):
            self.db.append_blobs_to_submodel(843, timestamp, submodel_blobs_list)
            out, _ = capsys.readouterr()
            assert out == "More than one patient entry found."

    def test_append_landmarks_to_model(self):
        internal_id = 850
        timestamp = 2
        path_fcsv = "tests/data/upload_data/00850/landmarks/pat850-ser002-heart-LV-apex.fcsv"
        name = "LV Apex"
        description = "LV Apex"
        type_of_spline = "marker_point"
        self.db.append_landmarks_to_model(
            internal_id=internal_id,
            timestamp=timestamp,
            path_fcsv=path_fcsv,
            name=name,
            description=description,
            type_of_spline=type_of_spline,
        )

        query = self.db.get_patient_model_list(internal_id=internal_id)
        query = next((item for item in query if item["timestamp"] == timestamp), None)
        assert query["landmarks"][0]["value"] == [[84.69594, -32.78431, 484.6335]]
        assert query["landmarks"][0]["name"] == name
        assert query["landmarks"][0]["description"] == description
        assert query["landmarks"][0]["type_of_spline"] == type_of_spline

    def test_upload_patients(self, capsys):
        human_id = 722
        series = "SER00004"
        self.db.upload_patients(self.jsondir, patients_list=[{human_id: series}])
        query = self.db.get_patient_collection(human_id, series)[0]
        assert query["internal_info"]["internal_id"] == human_id

        human_id = 726
        self.db.upload_patients(self.jsondir, internal_id_list=[human_id])
        query = self.db.get_patient_collection(human_id)[0]
        assert query["internal_info"]["internal_id"] == human_id

        with pytest.raises(SystemError):
            self.db.upload_patients(self.jsondir)
            out, _ = capsys.readouterr()
            assert out == "Provide patient list or internal id list."

        with pytest.raises(SystemError):
            self.db.upload_patients(
                self.jsondir,
                patients_list=[{human_id: series}],
                internal_id_list=[human_id],
            )
            out, _ = capsys.readouterr()
            assert out == "Either patient list or internal id list can be defined."

        with pytest.raises(SystemError):
            human_id = 722
            self.db.upload_patients(self.jsondir, internal_id_list=[human_id])
            out, _ = capsys.readouterr()
            assert (
                out
                == "Directories where found in {}. Provide patients_list.".format(
                    os.path.join(self.jsondir, human_id)
                )
            )

        with pytest.raises(SystemExit):
            human_id = 722
            series = "SER00004"
            self.db.upload_patients(self.jsondir, patients_list=[{human_id: series}])
            out, _ = capsys.readouterr()
            assert out == "Patient already present in the database."

        with pytest.raises(SystemExit):
            human_id = 726
            self.db.upload_patients(self.jsondir, internal_id_list=[human_id])
            out, _ = capsys.readouterr()
            assert out == "Patient already present in the database."

        with pytest.raises(FileNotFoundError):
            human_id = 726
            series = "SER00004"
            self.db.upload_patients(self.jsondir, patients_list=[{human_id: series}])
            out, _ = capsys.readouterr()
            assert out == os.path.join(
                self.jsondir, f"{int(human_id):05}", series, "patient_collection.json"
            )

    def test_upload_patients_add_to_cohort(self):
        cohort_id = "626aba549ce90c7ccbe9520e"
        human_id_1 = 122
        series_1 = "SER00012"
        human_id_2 = 693
        series_2 = "SER00302"

        self.db.upload_patients_add_to_cohort(
            self.jsondir,
            cohort_id,
            patients_list=[{human_id_1: series_1}, {human_id_2: series_2}],
        )

        query = self.db.get_patient_collection(human_id_1, series_1)[0]
        assert query["internal_info"]["internal_id"] == human_id_1
        query_2 = self.db.get_patient_collection(human_id_2, series_2)[0]
        assert query_2["internal_info"]["internal_id"] == human_id_2
        pat_ids = self.db.get_patient_cohort(cohort_id=cohort_id)["patient_ids"][-2:]
        query_3 = self.db.get_patient_collection(human_id_1, series_1)[0]
        assert str(query_3["_id"]) == pat_ids[0]
        query_4 = self.db.get_patient_collection(human_id_2, series_2)[0]
        assert str(query_4["_id"]) == pat_ids[1]
        query_5 = self.db.get_patient_cohort(cohort_id=cohort_id)["height"]
        query_6 = self.db.get_patient_cohort(cohort_id=cohort_id)["weight"]
        query_7 = self.db.get_patient_cohort(cohort_id=cohort_id)["number_patients"]
        assert query_5["min"] == 160.0 and query_5["max"] == 187.0
        assert query_6["min"] == 44 and query_6["max"] == 88
        assert query_7 == 3

    def test_set_max_min_patient_dimensions_in_cohort(self):
        human_id_1 = 122
        series_1 = "SER00012"
        human_id_2 = 693
        series_2 = "SER00302"
        human_id_3 = 722
        series_3 = "SER00004"
        cohort_id = "626aba549ce90c7ccbe9520e"

        database = DBOps("inmemory")
        database.upload_patients_add_to_cohort(
            self.jsondir, cohort_id, patients_list=[{human_id_1: series_1}]
        )
        cohort_doc = database.set_max_min_patient_dimensions_in_cohort(cohort_id)
        assert cohort_doc["height"]["min"] == cohort_doc["height"]["max"] == 187.0
        assert cohort_doc["weight"]["min"] == cohort_doc["weight"]["max"] == 88.0
        database.upload_patients_add_to_cohort(
            self.jsondir, cohort_id, patients_list=[{human_id_2: series_2}]
        )
        cohort_doc = database.set_max_min_patient_dimensions_in_cohort(cohort_id)
        assert (
            cohort_doc["height"]["min"] == 161.0
            and cohort_doc["height"]["max"] == 187.0
        )
        assert (
            cohort_doc["weight"]["min"] == 44.0 and cohort_doc["weight"]["max"] == 88.0
        )
        database.upload_patients_add_to_cohort(
            self.jsondir, cohort_id, patients_list=[{human_id_3: series_3}]
        )
        cohort_doc = database.set_max_min_patient_dimensions_in_cohort(cohort_id)
        assert (
            cohort_doc["height"]["min"] == 161.0
            and cohort_doc["height"]["max"] == 187.0
        )
        assert (
            cohort_doc["weight"]["min"] == 44.0 and cohort_doc["weight"]["max"] == 88.0
        )
