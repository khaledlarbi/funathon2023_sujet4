import s3fs
import pandas as pd
from download_pb import download_pb
from import_yaml import import_yaml

config = import_yaml("config.yaml")

DESTINATION_RAW = f"{BUCKET}/{config['DESTINATION_DATA_S3']}"

fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": config["ENDPOINT_S3"]}
)


# OPENFOOD -------------------------------------



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



# CATEGORIZATION OPENFOOD ---------------------------

download_pb("https://raw.githubusercontent.com/InseeFrLab/predicat/master/app/utils_ddc.py", "utils_ddc.py")

from utils_ddc import preprocess_text

openfood['preprocessed_labels'] = openfood['product_name'].str.upper().astype(str).apply(preprocess_text)





# Approche 1: binaire fasttext
from download_pb import download_pb

LOCATION_FASTTEXT_COICOP = "https://minio.lab.sspcloud.fr/projet-funathon/2023/sujet4/diffusion/model_coicop10.bin"


# methode 1: lien url
download_pb(url = LOCATION_FASTTEXT_COICOP, fname = "fasttext_coicop.bin")

# methode 2: s3fs
fs.download(LOCATION_FASTTEXT_COICOP.replace("https://minio.lab.sspcloud.fr/", ""), "fasttext_coicop.bin")

import fasttext

model = fasttext.load_model("fasttext_coicop.bin")

predictions = pd.DataFrame(
    {
    "coicop": \
        [k[0] for k in model.predict(
            [str(libel) for libel in openfood["preprocessed_labels"]], k = 1
            )[0]]
    })

openfood["coicop"] = predictions["coicop"].str.replace(r'__label__', '')


coicop = pd.read_excel("https://www.insee.fr/fr/statistiques/fichier/2402696/coicop2016_liste_n5.xls", skiprows=1)
coicop['Code'] = coicop['Code'].str.replace("'", "")

openfood = openfood.merge(coicop, how = "left", left_on = "coicop", right_on = "Code")
openfood = openfood.rename({"Libellé": "category"}, axis = "columns")
openfood = openfood.drop('Code', axis = "columns")

with fs.open(DESTINATION_OPENFOOD, "wb") as f:
    openfood.to_parquet(f)



