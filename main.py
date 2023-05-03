import streamlit as st
import duckdb
from detect_barcode import extract_ean, visualise_barcode

st.title('Hello streamlit')

#input_url = st.text_input('Renseigner un url', 'https://barcode-list.com/barcodeImage.php?barcode=3274080005003')

with st.sidebar:
    input_url = st.file_uploader("Uploaded une photo:", accept_multiple_files=False)
    st.image(input_url)


decoded_objects = extract_ean(input_url)
ean = decoded_objects[0].data.decode("utf-8")


st.write('EAN détecté:', ean)

# partie 2: retrouver le produit depuis openfood

import requests
import pandas as pd
import s3fs
from detect_barcode import extract_ean

ENDPOINT_URL = "https://minio.lab.sspcloud.fr"
BUCKET = "projet-funathon"
URL = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
filename = URL.rsplit("/", maxsplit=1)[-1]
BUCKET_RAW = f"{BUCKET}/2023/sujet4/diffusion"


con = duckdb.connect(database=':memory:')
con.execute("""
INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint='minio.lab.sspcloud.fr'
""")



@st.cache_data
def load_data(ean):
    openfood_data = con.sql(f"select * from read_parquet('temp.parquet') WHERE CAST(ltrim(code, '0') AS STRING) = CAST(ltrim({ean}) AS STRING)")
    return openfood_data.df()

subset = load_data(ean)

st.write('Consulter ce produit sur le site openfoodfacts:', subset["url"].iloc[0])
st.image(subset["image_url"].iloc[0])


info_nutritionnelles = [
    'energy-kcal_100g', 'fat_100g', 'saturated-fat_100g',
    'carbohydrates_100g', 'sugars_100g',
    'proteins_100g', 'salt_100g']

subset2 = subset.loc[:, ['code', 'product_name', 'category'] + info_nutritionnelles]
st.dataframe(subset2)

