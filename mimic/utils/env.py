import os
from dotenv import load_dotenv


class Env:
    def __init__(self) -> None:
        self.credentials = {
            "username": os.environ.get("USERNAME"),
            "password": os.environ.get("PASSWORD"),
        }

        self.iv_version = os.environ.get("IV_VERSION", "3.1")
        self.iv_url = f"https://physionet.org/files/mimiciv/{self.iv_version}"
        self.iv_files = [
            {
                "name": "admissions",
                "url": f"{self.iv_url}/hosp/admissions.csv.gz",
                "md5": os.environ.get("IV_admissions.csv.gz_MD5"),
            },
            {
                "name": "d_hcpcs",
                "url": f"{self.iv_url}/hosp/d_hcpcs.csv.gz",
                "md5": os.environ.get("IV_d_hcpcs.csv.gz_MD5"),
            },
            {
                "name": "d_icd_diagnoses",
                "url": f"{self.iv_url}/hosp/d_icd_diagnoses.csv.gz",
                "md5": os.environ.get("IV_d_icd_diagnoses.csv.gz_MD5"),
            },
            {
                "name": "d_icd_procedures",
                "url": f"{self.iv_url}/hosp/d_icd_procedures.csv.gz",
                "md5": os.environ.get("IV_d_icd_procedures.csv.gz_MD5"),
            },
            {
                "name": "d_labitems",
                "url": f"{self.iv_url}/hosp/d_labitems.csv.gz",
                "md5": os.environ.get("IV_d_labitems.csv.gz_MD5"),
            },
            {
                "name": "diagnoses_icd",
                "url": f"{self.iv_url}/hosp/diagnoses_icd.csv.gz",
                "md5": os.environ.get("IV_diagnoses_icd.csv.gz_MD5"),
            },
            {
                "name": "drgcodes",
                "url": f"{self.iv_url}/hosp/drgcodes.csv.gz",
                "md5": os.environ.get("IV_drgcodes.csv.gz_MD5"),
            },
            {
                "name": "emar",
                "url": f"{self.iv_url}/hosp/emar.csv.gz",
                "md5": os.environ.get("IV_emar.csv.gz_MD5"),
            },
            {
                "name": "emar_detail",
                "url": f"{self.iv_url}/hosp/emar_detail.csv.gz",
                "md5": os.environ.get("IV_emar_detail.csv.gz_MD5"),
            },
            {
                "name": "hcpcsevents",
                "url": f"{self.iv_url}/hosp/hcpcsevents.csv.gz",
                "md5": os.environ.get("IV_hcpcsevents.csv.gz_MD5"),
            },
            {
                "name": "labevents",
                "url": f"{self.iv_url}/hosp/labevents.csv.gz",
                "md5": os.environ.get("IV_labevents.csv.gz_MD5"),
            },
            {
                "name": "microbiologyevents",
                "url": f"{self.iv_url}/hosp/microbiologyevents.csv.gz",
                "md5": os.environ.get("IV_microbiologyevents.csv.gz_MD5"),
            },
            {
                "name": "omr",
                "url": f"{self.iv_url}/hosp/omr.csv.gz",
                "md5": os.environ.get("IV_omr.csv.gz_MD5"),
            },
            {
                "name": "patients",
                "url": f"{self.iv_url}/hosp/patients.csv.gz",
                "md5": os.environ.get("IV_patients.csv.gz_MD5"),
            },
            {
                "name": "pharmacy",
                "url": f"{self.iv_url}/hosp/pharmacy.csv.gz",
                "md5": os.environ.get("IV_pharmacy.csv.gz_MD5"),
            },
            {
                "name": "poe",
                "url": f"{self.iv_url}/hosp/poe.csv.gz",
                "md5": os.environ.get("IV_poe.csv.gz_MD5"),
            },
            {
                "name": "poe_detail",
                "url": f"{self.iv_url}/hosp/poe_detail.csv.gz",
                "md5": os.environ.get("IV_poe_detail.csv.gz_MD5"),
            },
            {
                "name": "prescriptions",
                "url": f"{self.iv_url}/hosp/prescriptions.csv.gz",
                "md5": os.environ.get("IV_prescriptions.csv.gz_MD5"),
            },
            {
                "name": "procedures_icd",
                "url": f"{self.iv_url}/hosp/procedures_icd.csv.gz",
                "md5": os.environ.get("IV_procedures_icd.csv.gz_MD5"),
            },
            {
                "name": "provider",
                "url": f"{self.iv_url}/hosp/provider.csv.gz",
                "md5": os.environ.get("IV_provider.csv.gz_MD5"),
            },
            {
                "name": "services",
                "url": f"{self.iv_url}/hosp/services.csv.gz",
                "md5": os.environ.get("IV_services.csv.gz_MD5"),
            },
            {
                "name": "transfers",
                "url": f"{self.iv_url}/hosp/transfers.csv.gz",
                "md5": os.environ.get("IV_transfers.csv.gz_MD5"),
            },
            {
                "name": "caregiver",
                "url": f"{self.iv_url}/icu/caregiver.csv.gz",
                "md5": os.environ.get("IV_caregiver.csv.gz_MD5"),
            },
            {
                "name": "chartevents",
                "url": f"{self.iv_url}/icu/chartevents.csv.gz",
                "md5": os.environ.get("IV_chartevents.csv.gz_MD5"),
            },
            {
                "name": "d_items",
                "url": f"{self.iv_url}/icu/d_items.csv.gz",
                "md5": os.environ.get("IV_d_items.csv.gz_MD5"),
            },
            {
                "name": "datetimeevents",
                "url": f"{self.iv_url}/icu/datetimeevents.csv.gz",
                "md5": os.environ.get("IV_datetimeevents.csv.gz_MD5"),
            },
            {
                "name": "icustays",
                "url": f"{self.iv_url}/icu/icustays.csv.gz",
                "md5": os.environ.get("IV_icustays.csv.gz_MD5"),
            },
            {
                "name": "ingredientevents",
                "url": f"{self.iv_url}/icu/ingredientevents.csv.gz",
                "md5": os.environ.get("IV_ingredientevents.csv.gz_MD5"),
            },
            {
                "name": "inputevents",
                "url": f"{self.iv_url}/icu/inputevents.csv.gz",
                "md5": os.environ.get("IV_inputevents.csv.gz_MD5"),
            },
            {
                "name": "outputevents",
                "url": f"{self.iv_url}/icu/outputevents.csv.gz",
                "md5": os.environ.get("IV_outputevents.csv.gz_MD5"),
            },
            {
                "name": "procedureevents",
                "url": f"{self.iv_url}/icu/procedureevents.csv.gz",
                "md5": os.environ.get("IV_procedureevents.csv.gz_MD5"),
            },
        ]

        self.cxr_version = os.environ.get("CXR_VERSION", "2.1.0")
        self.cxr_url = f"https://physionet.org/files/mimic-cxr-jpg/{self.cxr_version}"
        self.cxr_files = [
            {
                "name": "chexpert",
                "url": f"{self.cxr_url}/mimic-cxr-2.0.0-chexpert.csv.gz",
                "md5": os.environ.get("CXR_chexpert.csv.gz_MD5"),
            },
            {
                "name": "metadata",
                "url": f"{self.cxr_url}/mimic-cxr-2.0.0-metadata.csv.gz",
                "md5": os.environ.get("CXR_metadata.csv.gz_MD5"),
            },
            {
                "name": "negbio",
                "url": f"{self.cxr_url}/mimic-cxr-2.0.0-negbio.csv.gz",
                "md5": os.environ.get("CXR_negbio.csv.gz_MD5"),
            },
            {
                "name": "split",
                "url": f"{self.cxr_url}/mimic-cxr-2.0.0-split.csv.gz",
                "md5": os.environ.get("CXR_split.csv.gz_MD5"),
            },
            {
                "name": "test-set-labeled",
                "url": f"{self.cxr_url}/mimic-cxr-2.1.0-test-set-labeled.csv.gz",
                "md5": os.environ.get("CXR_test-set-labeled.csv.gz_MD5"),
            },
        ]

    @staticmethod
    def load() -> None:
        load_dotenv()
