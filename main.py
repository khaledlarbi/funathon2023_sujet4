import streamlit as st
from detect_barcode import extract_ean, visualise_barcode

st.title('Détection de code barres')

input_url = st.text_input('Renseigner un url', '')


st.image(input_url)

decoded_objects = extract_ean(input_url)
ean = decoded_objects[0].data.decode("utf-8")


st.write('Detecter EAN:', ean)

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


@st.cache_data
def load_data():
    openfood_data = pd.read_parquet("temp.parquet")  # 👈 Download the data
    return openfood_data

openfood_data = load_data()

st.button("Rerun")

subset = openfood_data.loc[openfood_data["code"] == ean]

st.write('Consulter ce produit sur le site openfoodfacts:', subset["url"])

subset2 = subset.loc[:, ['code','']]
