import s3fs
import pandas as pd
from utils.download_pb import download_pb
from utils.import_yaml import import_yaml

config = import_yaml("config.yaml")

DESTINATION_RAW = f"{config['BUCKET']}/{config['DESTINATION_DATA_S3']}"
DESTINATION_OPENFOOD = f"{DESTINATION_RAW}/openfood.parquet"

fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": config["ENDPOINT_S3"]}
)


# OPENFOOD -------------------------------------

def download_openfood(url: str = config["URL_OPENFOOD"], destination: str = "openfood.csv"):
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
