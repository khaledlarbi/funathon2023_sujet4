from detect_barcode import extract_ean


import s3fs
import pandas as pd


ENDPOINT_URL = "https://minio.lab.sspcloud.fr"
BUCKET = "projet-formation"
BUCKET_RAW = f"{BUCKET}/diffusion/funathon/sujet4"

fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL})


with fs.open(f"{BUCKET_RAW}/openfood.parquet", "rb") as remote_file:
    openfood = pd.read_parquet(remote_file)

url = "https://barcode-list.com/barcodeImage.php?barcode=5000112602999"


decoded_objects = extract_ean(url)

ean = decoded_objects[0].data.decode("utf-8")

match_data = openfood.loc[openfood["code"] == ean]
info_nutritionnelles = match_data.columns[match_data.columns.str.contains("_100g")].tolist()

data_to_categorize = match_data.loc[:, ["code", "product_name"] + info_nutritionnelles]

product_name = data_to_categorize['product_name'].iloc[0]
url_api = f"https://api.lab.sspcloud.fr/predicat/label?k=1&q=%27{product_name}%27"


import requests

output_api_predicat = requests.get(url_api).json()
coicop_found = output_api_predicat['coicop'][f"'{product_name}'"][0]['label']
coicop_found

coicop = pd.read_excel("https://www.insee.fr/fr/statistiques/fichier/2402696/coicop2016_liste_n5.xls", skiprows=1)
coicop['Code'] = coicop['Code'].str.replace("'", "")

data_to_categorize['category'] = coicop.loc[coicop['Code'] == coicop_found]['Libellé'].iloc[0]