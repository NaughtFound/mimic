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
        self.iv_files = {
            "admissions": {
                "name": "admissions.csv",
                "url": f"{self.iv_url}/hosp/admissions.csv.gz",
                "md5": os.environ.get("IV_admissions.csv.gz_MD5"),
            },
            "d_hcpcs": {
                "name": "d_hcpcs.csv",
                "url": f"{self.iv_url}/hosp/d_hcpcs.csv.gz",
                "md5": os.environ.get("IV_d_hcpcs.csv.gz_MD5"),
            },
            "d_icd_diagnoses": {
                "name": "d_icd_diagnoses.csv",
                "url": f"{self.iv_url}/hosp/d_icd_diagnoses.csv.gz",
                "md5": os.environ.get("IV_d_icd_diagnoses.csv.gz_MD5"),
            },
            "d_icd_procedures": {
                "name": "d_icd_procedures.csv",
                "url": f"{self.iv_url}/hosp/d_icd_procedures.csv.gz",
                "md5": os.environ.get("IV_d_icd_procedures.csv.gz_MD5"),
            },
            "d_labitems": {
                "name": "d_labitems.csv",
                "url": f"{self.iv_url}/hosp/d_labitems.csv.gz",
                "md5": os.environ.get("IV_d_labitems.csv.gz_MD5"),
            },
            "diagnoses_icd": {
                "name": "diagnoses_icd.csv",
                "url": f"{self.iv_url}/hosp/diagnoses_icd.csv.gz",
                "md5": os.environ.get("IV_diagnoses_icd.csv.gz_MD5"),
            },
            "drgcodes": {
                "name": "drgcodes.csv",
                "url": f"{self.iv_url}/hosp/drgcodes.csv.gz",
                "md5": os.environ.get("IV_drgcodes.csv.gz_MD5"),
            },
            "emar": {
                "name": "emar.csv",
                "url": f"{self.iv_url}/hosp/emar.csv.gz",
                "md5": os.environ.get("IV_emar.csv.gz_MD5"),
            },
            "emar_detail": {
                "name": "emar_detail.csv",
                "url": f"{self.iv_url}/hosp/emar_detail.csv.gz",
                "md5": os.environ.get("IV_emar_detail.csv.gz_MD5"),
            },
            "hcpcsevents": {
                "name": "hcpcsevents.csv",
                "url": f"{self.iv_url}/hosp/hcpcsevents.csv.gz",
                "md5": os.environ.get("IV_hcpcsevents.csv.gz_MD5"),
            },
            "labevents": {
                "name": "labevents.csv",
                "url": f"{self.iv_url}/hosp/labevents.csv.gz",
                "md5": os.environ.get("IV_labevents.csv.gz_MD5"),
            },
            "microbiologyevents": {
                "name": "microbiologyevents.csv",
                "url": f"{self.iv_url}/hosp/microbiologyevents.csv.gz",
                "md5": os.environ.get("IV_microbiologyevents.csv.gz_MD5"),
            },
            "omr": {
                "name": "omr.csv",
                "url": f"{self.iv_url}/hosp/omr.csv.gz",
                "md5": os.environ.get("IV_omr.csv.gz_MD5"),
            },
            "patients": {
                "name": "patients.csv",
                "url": f"{self.iv_url}/hosp/patients.csv.gz",
                "md5": os.environ.get("IV_patients.csv.gz_MD5"),
            },
            "pharmacy": {
                "name": "pharmacy.csv",
                "url": f"{self.iv_url}/hosp/pharmacy.csv.gz",
                "md5": os.environ.get("IV_pharmacy.csv.gz_MD5"),
            },
            "poe": {
                "name": "poe.csv",
                "url": f"{self.iv_url}/hosp/poe.csv.gz",
                "md5": os.environ.get("IV_poe.csv.gz_MD5"),
            },
            "poe_detail": {
                "name": "poe_detail.csv",
                "url": f"{self.iv_url}/hosp/poe_detail.csv.gz",
                "md5": os.environ.get("IV_poe_detail.csv.gz_MD5"),
            },
            "prescriptions": {
                "name": "prescriptions.csv",
                "url": f"{self.iv_url}/hosp/prescriptions.csv.gz",
                "md5": os.environ.get("IV_prescriptions.csv.gz_MD5"),
            },
            "procedures_icd": {
                "name": "procedures_icd.csv",
                "url": f"{self.iv_url}/hosp/procedures_icd.csv.gz",
                "md5": os.environ.get("IV_procedures_icd.csv.gz_MD5"),
            },
            "provider": {
                "name": "provider.csv",
                "url": f"{self.iv_url}/hosp/provider.csv.gz",
                "md5": os.environ.get("IV_provider.csv.gz_MD5"),
            },
            "services": {
                "name": "services.csv",
                "url": f"{self.iv_url}/hosp/services.csv.gz",
                "md5": os.environ.get("IV_services.csv.gz_MD5"),
            },
            "transfers": {
                "name": "transfers.csv",
                "url": f"{self.iv_url}/hosp/transfers.csv.gz",
                "md5": os.environ.get("IV_transfers.csv.gz_MD5"),
            },
            "caregiver": {
                "name": "caregiver.csv",
                "url": f"{self.iv_url}/icu/caregiver.csv.gz",
                "md5": os.environ.get("IV_caregiver.csv.gz_MD5"),
            },
            "chartevents": {
                "name": "chartevents.csv",
                "url": f"{self.iv_url}/icu/chartevents.csv.gz",
                "md5": os.environ.get("IV_chartevents.csv.gz_MD5"),
            },
            "d_items": {
                "name": "d_items.csv",
                "url": f"{self.iv_url}/icu/d_items.csv.gz",
                "md5": os.environ.get("IV_d_items.csv.gz_MD5"),
            },
            "datetimeevents": {
                "name": "datetimeevents.csv",
                "url": f"{self.iv_url}/icu/datetimeevents.csv.gz",
                "md5": os.environ.get("IV_datetimeevents.csv.gz_MD5"),
            },
            "icustays": {
                "name": "icustays.csv",
                "url": f"{self.iv_url}/icu/icustays.csv.gz",
                "md5": os.environ.get("IV_icustays.csv.gz_MD5"),
            },
            "ingredientevents": {
                "name": "ingredientevents.csv",
                "url": f"{self.iv_url}/icu/ingredientevents.csv.gz",
                "md5": os.environ.get("IV_ingredientevents.csv.gz_MD5"),
            },
            "inputevents": {
                "name": "inputevents.csv",
                "url": f"{self.iv_url}/icu/inputevents.csv.gz",
                "md5": os.environ.get("IV_inputevents.csv.gz_MD5"),
            },
            "outputevents": {
                "name": "outputevents.csv",
                "url": f"{self.iv_url}/icu/outputevents.csv.gz",
                "md5": os.environ.get("IV_outputevents.csv.gz_MD5"),
            },
            "procedureevents": {
                "name": "procedureevents.csv",
                "url": f"{self.iv_url}/icu/procedureevents.csv.gz",
                "md5": os.environ.get("IV_procedureevents.csv.gz_MD5"),
            },
        }

        self.cxr_version = os.environ.get("CXR_VERSION", "2.1.0")
        self.cxr_url = f"https://physionet.org/files/mimic-cxr-jpg/{self.cxr_version}"
        self.cxr_files = {
            "chexpert": {
                "name": "mimic-cxr-2.0.0-chexpert.csv",
                "url": f"{self.cxr_url}/mimic-cxr-2.0.0-chexpert.csv.gz",
                "md5": os.environ.get("CXR_chexpert.csv.gz_MD5"),
            },
            "metadata": {
                "name": "mimic-cxr-2.0.0-metadata.csv",
                "url": f"{self.cxr_url}/mimic-cxr-2.0.0-metadata.csv.gz",
                "md5": os.environ.get("CXR_metadata.csv.gz_MD5"),
            },
            "negbio": {
                "name": "mimic-cxr-2.0.0-negbio.csv",
                "url": f"{self.cxr_url}/mimic-cxr-2.0.0-negbio.csv.gz",
                "md5": os.environ.get("CXR_negbio.csv.gz_MD5"),
            },
            "split": {
                "name": "mimic-cxr-2.0.0-split.csv",
                "url": f"{self.cxr_url}/mimic-cxr-2.0.0-split.csv.gz",
                "md5": os.environ.get("CXR_split.csv.gz_MD5"),
            },
            "test-set-labeled": {
                "name": "mimic-cxr-2.1.0-test-set-labeled.csv",
                "url": f"{self.cxr_url}/mimic-cxr-2.1.0-test-set-labeled.csv.gz",
                "md5": os.environ.get("CXR_test-set-labeled.csv.gz_MD5"),
            },
        }

    @staticmethod
    def load() -> None:
        load_dotenv()
