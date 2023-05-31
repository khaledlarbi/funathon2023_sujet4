import streamlit as st
import cv2
import duckdb
import requests
import pandas as pd
import s3fs

from detect_barcode import extract_ean, visualise_barcode

from utils.import_yaml import import_yaml
config = import_yaml("config.yaml")

INPUT_OPENFOOD = f"{config['BUCKET']}{config['DESTINATION_DATA_S3']}/openfood.parquet"

info_nutritionnelles = [
    'energy-kcal_100g', 'fat_100g', 'saturated-fat_100g',
    'carbohydrates_100g', 'sugars_100g',
    'proteins_100g', 'salt_100g']

st.title('Hello streamlit')

#input_url = st.text_input('Renseigner un url', 'https://barcode-list.com/barcodeImage.php?barcode=3274080005003')

with st.sidebar:
    input_url = st.file_uploader("Uploaded une photo:", accept_multiple_files=False)
    if input_url is not None:
        img = visualise_barcode(input_url)
        cv2.imwrite('barcode_opencv.jpg', img)
        st.image('barcode_opencv.jpg')

if input_url is not None:
    decoded_objects = extract_ean(input_url)
    ean = decoded_objects[0].data.decode("utf-8")
    st.write('EAN détecté:', ean)
else:
    st.write("Fournir une image")
# partie 2: retrouver le produit depuis openfood


con = duckdb.connect(database=':memory:')
con.execute("""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_endpoint='minio.lab.sspcloud.fr'
""")


URL_PARQUET = "https://minio.lab.sspcloud.fr/projet-funathon/2023/sujet4/diffusion/openfood.parquet"

info_nutritionnelles = [
    'energy-kcal_100g', 'fat_100g', 'saturated-fat_100g',
    'carbohydrates_100g', 'sugars_100g',
    'proteins_100g', 'salt_100g']
indices_synthetiques = [
    "nutriscore_grade", "ecoscore_grade", "nova_group"
]
principales_infos = ['product_name', 'code', 'preprocessed_labels', 'coicop', "url", "image_url"]
liste_colonnes = principales_infos + indices_synthetiques + info_nutritionnelles
liste_colonnes_sql = [f"\"{s}\"" for s in liste_colonnes]
liste_colonnes_sql = ', '.join(liste_colonnes_sql)

@st.cache_data
def load_data(ean):
    openfood_data = con.sql(
        f"SELECT {liste_colonnes_sql} FROM read_parquet('s3://{INPUT_OPENFOOD}') WHERE CAST(ltrim(code, '0') AS STRING) = CAST(ltrim({ean}) AS STRING)"
        ).df()
    return openfood_data

if input_url is not None:
    subset = load_data(ean)
else:
    st.write('Produit exemple: Coca-Cola')
    subset = load_data("5000112602791")

st.write('Consulter ce produit sur le site openfoodfacts:', subset["url"].iloc[0])
st.image(subset["image_url"].iloc[0])




subset2 = subset.loc[:, ['code', 'product_name', 'coicop'] + info_nutritionnelles]
st.dataframe(subset2)

