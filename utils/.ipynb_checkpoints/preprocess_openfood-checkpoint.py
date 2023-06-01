import os
import numpy as np
import s3fs
import pandas as pd
import fasttext

from utils.download_pb import download_pb
from utils.import_yaml import import_yaml
from utils.utils_ddc import preprocess_text

config = import_yaml("config.yaml")

DESTINATION_RAW = f"{config['BUCKET']}/{config['DESTINATION_DATA_S3']}"
DESTINATION_OPENFOOD = f"{DESTINATION_RAW}/openfood.parquet"

fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": config["ENDPOINT_S3"]}
)


# OPENFOOD -------------------------------------

def download_openfood(
        url: str = config["URL_OPENFOOD"],
        destination: str = "openfood.csv",
        force: bool = False
    ):
    
    if os.path.exists(destination) is True and force is False:
        print(f"{destination} exists, if you want to override, add argument force=True")
        return None
    
    download_pb(url, destination)

    
def import_openfood(filename):
    openfood = pd.read_csv(
        filename,
        delimiter="\t",
        encoding="utf-8",
        dtype={
            "code ": "str",
            "emb_codes": "str",
            "emb_codes_tags": "str",
            "energy_100g": "float",
            "alcohol_100g": "float",
        },
        parse_dates=[
            "created_datetime",
            "last_modified_datetime",
            "last_image_datetime"
            ],
    )

    openfood["code"] = openfood["code"].astype(str)
    
    return openfood


def clean_column_dataset(
    data: pd.DataFrame,
    dict_rules_replacement: dict,
    variable_to_clean: str, variable_output: str) :
    
    data[variable_output] = data[variable_to_clean].str.upper()
    data = data.replace({variable_output: dict_rules_replacement}, regex=True)
    return data


def import_coicop_labels(url: str) -> pd.DataFrame:
    coicop = pd.read_excel(url, skiprows=1)
    coicop['Code'] = coicop['Code'].str.replace("'", "")
    coicop = coicop.rename({"Libellé": "category"}, axis = "columns")
    return coicop


def model_predict_coicop(data, model, product_column: str = "preprocessed_labels", output_column: str = "coicop"):
    predictions = pd.DataFrame(
        {
        output_column: \
            [k[0] for k in model.predict(
                [str(libel) for libel in data[product_column]], k = 1
                )[0]]
        })

    data[output_column] = predictions[output_column].str.replace(r'__label__', '')
    return data

