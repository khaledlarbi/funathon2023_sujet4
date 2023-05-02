import s3fs
import pandas as pd
from download_pb import download_pb

ENDPOINT_URL = "https://minio.lab.sspcloud.fr"
BUCKET = "projet-funathon"
URL = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
filename = URL.rsplit("/", maxsplit=1)[-1]
DESTINATION_RAW = f"{BUCKET}/2023/sujet4/diffusion"

fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL})


# OPENFOOD -------------------------------------

DESTINATION_OPENFOOD = f"{DESTINATION_RAW}/openfood.parquet"


download_pb(URL, filename)

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

with fs.open(DESTINATION_OPENFOOD, "wb") as f:
    openfood.to_parquet(f)
