import requests
import pandas as pd
import s3fs

from detect_barcode import extract_ean


ENDPOINT_URL = "https://minio.lab.sspcloud.fr"
BUCKET = "projet-formation"
BUCKET_RAW = f"{BUCKET}/diffusion/funathon/sujet4"

fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL})


# DECODE A GIVEN IMAGE -----------------------

url = "https://barcode-list.com/barcodeImage.php?barcode=5000112602999"

decoded_objects = extract_ean(url)

ean = decoded_objects[0].data.decode("utf-8")

# FIND AN ECHO --------------------------

with fs.open(f"{BUCKET_RAW}/openfood.parquet", "rb") as remote_file:
    openfood = pd.read_parquet(remote_file)

info_nutritionnelles = openfood.filter(like = "_100g").columns.tolist()
list_columns = ["code", "product_name"] + info_nutritionnelles


# VERSION 1: PANDAS
match_data = openfood.loc[openfood["code"] == ean]
data_to_categorize = match_data.loc[:, list_columns]

# VERSION 2: DUCKDB
import duckdb
con = duckdb.connect(database=':memory:')
con.execute("""
INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint='minio.lab.sspcloud.fr'
""")
openfood_head = con.sql(f"select * from read_parquet('s3://{BUCKET_RAW}/openfood.parquet') LIMIT 1").df()
info_nutritionnelles = openfood_head.filter(like = "_100g").columns.tolist()
list_columns = ["code", "product_name"] + info_nutritionnelles
subset_columns = ", ".join([f"\"{s}\"" for s in list_columns])

out = con.sql(f"select {subset_columns} from read_parquet('s3://{BUCKET_RAW}/openfood.parquet') WHERE CAST(ltrim(code, '0') AS STRING) = CAST(ltrim({ean}) AS STRING)")
data_to_categorize = out.df()

data_to_categorize
product_name = data_to_categorize['product_name'].iloc[0]


# RECHERCHE TEXTUELLE ------------------------------

out_textual = con.sql(f"select {subset_columns} from read_parquet('s3://{BUCKET_RAW}/openfood.parquet') WHERE jaro_winkler_similarity('{product_name}',product_name) > 0.9")
out_textual = out_textual.df()

out_textual_imputed = pd.concat(
    [
        data_to_categorize.loc[:, ["code", "product_name"]].reset_index(drop = True),
        pd.DataFrame(out_textual.loc[:, info_nutritionnelles].mean()).T
    ], ignore_index=True, axis=1
)
out_textual_imputed.columns = data_to_categorize.columns.tolist()

data_to_categorize = out_textual_imputed.copy()

# CATEGORIZATION OPENFOOD ---------------------------

# Approche 1: binaire fasttext
from download_pb import download_pb

LOCATION_FASTTEXT_COICOP = "https://minio.lab.sspcloud.fr/projet-funathon/2023/sujet4/diffusion/model_coicop10.bin"

# methode 1: lien url
download_pb(url = LOCATION_FASTTEXT_COICOP, fname = "fasttext_coicop.bin")

# methode 2: s3fs
fs.download(LOCATION_FASTTEXT_COICOP, "fasttext_coicop.bin")




# PREDICAT ------------------------------------------

url_api = f"https://api.lab.sspcloud.fr/predicat/label?k=1&q=%27{product_name}%27"



output_api_predicat = requests.get(url_api).json()
coicop_found = output_api_predicat['coicop'][f"'{product_name}'"][0]['label']
coicop_found

coicop = pd.read_excel("https://www.insee.fr/fr/statistiques/fichier/2402696/coicop2016_liste_n5.xls", skiprows=1)
coicop['Code'] = coicop['Code'].str.replace("'", "")

data_to_categorize['category'] = coicop.loc[coicop['Code'] == coicop_found]['Libellé'].iloc[0]