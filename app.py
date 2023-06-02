import streamlit as st
import cv2
import pandas as pd

from utils.detect_barcode import extract_ean, visualise_barcode
from utils.pipeline import find_product_openfood
from utils.construct_figures import plot_product_info

info_nutritionnelles = [
    'energy-kcal_100g', 'fat_100g', 'saturated-fat_100g',
    'carbohydrates_100g', 'sugars_100g',
    'proteins_100g', 'salt_100g']

st.title('Mon Yuka 🥕 avec Python 🐍')

#input_url = st.text_input('Renseigner un url', 'https://barcode-list.com/barcodeImage.php?barcode=3274080005003')
def label_grade_formatter(s):
    return s.split("_", maxsplit = 1)[0].capitalize()


def local_css(file_name):
    with open(file_name) as f:
        st.markdown('<style>{}</style>'.format(f.read()), unsafe_allow_html=True)
local_css("style.css")


with st.sidebar:
    input_method = st.radio(
        "What\'s your favorite movie genre",
        ('Photo enregistrée', 'Capture de la webcam'))
    if input_method == 'Photo enregistrée':
        input_url = st.file_uploader("Uploaded une photo:", accept_multiple_files=False)
    else:
        picture = st.camera_input("Take a picture")
        input_url = picture
    if input_url is not None:
        img = visualise_barcode(input_url)
        cv2.imwrite('barcode_opencv.jpg', img)
        st.image('barcode_opencv.jpg')
    options = st.multiselect(
        'Quelles statistiques afficher ?',
        ["nutriscore_grade", "nova_group", "ecoscore_grade"],
        ["nutriscore_grade", "nova_group", "ecoscore_grade"],
        format_func = label_grade_formatter)


if input_url is not None:
    decoded_objects = extract_ean(input_url)
    ean = decoded_objects[0].data.decode("utf-8")
    st.write('EAN détecté:', ean)


st.write(
    '<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">',
    unsafe_allow_html=True
)

# partie 2: retrouver le produit depuis openfood



stats_notes = pd.read_parquet("https://minio.lab.sspcloud.fr/projet-funathon/2023/sujet4/diffusion/stats_notes_pandas.parquet")


@st.cache_data
def load_data(ean):
    openfood_data = find_product_openfood(ean)
    return openfood_data

if input_url is not None:
    subset = load_data(ean)
else:
    st.write('Produit exemple: Coca-Cola')
    subset = load_data("5000112602791")

st.write('Consulter ce produit sur le site openfoodfacts:', subset["url"].iloc[0])
st.image(subset["image_url"].iloc[0])

st.dataframe(subset.loc[:, ~subset.columns.str.contains("url")])

t = f"<div>Statistiques parmi les <span class='highlight blue'>{subset['category'].iloc[0]}<span class='bold'>COICOP</span>"

st.markdown(t, unsafe_allow_html=True)


for var in options:
    fig = plot_product_info(subset, var, stats_notes)
    st.plotly_chart(fig, height=800)

