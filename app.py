import streamlit as st
from streamlit_javascript import st_javascript

import cv2
import pandas as pd
import duckdb

from utils.detect_barcode import extract_ean, visualise_barcode
from utils.pipeline import find_product_openfood
from utils.construct_figures import plot_product_info
from utils.utils_app import local_css, label_grade_formatter
from utils.download_pb import import_coicop_labels

# Une personnalisation sympa pour l'onglet
st.set_page_config(page_title="PYuka", page_icon="🍎")


# --------------------
# METADATA
indices_synthetiques = [
    "nutriscore_grade", "ecoscore_grade", "nova_group"
]
principales_infos = [
    'product_name', 'code', 'preprocessed_labels', 'coicop', \
    'url', 'image_url'
]
liste_colonnes = principales_infos + indices_synthetiques
liste_colonnes_sql = [f"\"{s}\"" for s in liste_colonnes]
liste_colonnes_sql = ', '.join(liste_colonnes_sql)

con = duckdb.connect(database=':memory:')
con.execute("""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_endpoint='minio.lab.sspcloud.fr'
""")

# LOAD DATASET
url_data = "https://projet-funathon.minio.lab.sspcloud.fr/2023/sujet4/diffusion/openfood.parquet"
stats_notes = pd.read_parquet("https://minio.lab.sspcloud.fr/projet-funathon/2023/sujet4/diffusion/stats_notes_pandas.parquet")
coicop = import_coicop_labels(
    "https://www.insee.fr/fr/statistiques/fichier/2402696/coicop2016_liste_n5.xls"
)

# --------------------


st.title('Mon Yuka 🥕 avec Python 🐍')

# Feuille de style & taille de l'écran pour adapter l'interface
local_css("style.css")
width = st_javascript(
    "window.innerWidth"
)

# --------------------------------
# PARTIE 1: LES INPUTS

if width > 500:
    # pour les grands écrans on met une partie à gauche
    # qui centralise plusieurs type d'input
    with st.sidebar:
        # choix de la méthode d'upload
        input_method = st.radio(
                "Méthode d'upload de la photo",
                ('Photo enregistrée', 'Capture de la webcam'))
        if input_method == 'Photo enregistrée':
            # file uploader
            input_url = st.file_uploader("Uploaded une photo:", accept_multiple_files=False)
        else:
            # camera uploader
            picture = st.camera_input("Take a picture")
            input_url = picture
        
        if input_url is not None:
            # visualise l'image s'il y a un input
            img = visualise_barcode(input_url)
            cv2.imwrite('barcode_opencv.jpg', img)
            st.image('barcode_opencv.jpg')

        # choix des statistiques à afficher
        options = st.multiselect(
                'Quelles statistiques afficher ?',
                ["nutriscore_grade", "nova_group", "ecoscore_grade"],
                ["nutriscore_grade", "nova_group", "ecoscore_grade"],
                format_func=label_grade_formatter)
else:
    # pour les petits écrans (type smartphone)
    # le file uploader est au début
    input_method = st.radio(
                    "Méthode d'upload de la photo",
                    ('Photo enregistrée', 'Capture de la webcam'))
    if input_method == 'Photo enregistrée':
        # file uploader
        input_url = st.file_uploader("Uploaded une photo:", accept_multiple_files=False)
    else:
        # camera uploader
        picture = st.camera_input("Take a picture")
        input_url = picture
    # choix des statistiques à afficher
    options = st.multiselect(
                    'Quelles statistiques afficher ?',
                    ["nutriscore_grade", "nova_group", "ecoscore_grade"],
                    ["nutriscore_grade", "nova_group", "ecoscore_grade"],
                    format_func=label_grade_formatter)


# ----------------------------------------------------------
# PARTIE 2: EXPLOITATION DES INPUTS DANS NOTRE APP



# CHARGEMENT DE LA LIGNE DANS OPENFOODFACTS
@st.cache_data
def load_data(ean):
    openfood_data = find_product_openfood(con, liste_colonnes_sql, url_data, ean, coicop)
    return openfood_data

if input_url is None:
    # Showcase product
    st.write('Produit exemple: Coca-Cola')
    subset = load_data("5000112602791")
    decoded_objects = extract_ean(subset["image_url"].iloc[0])
else:
    # decode image
    decoded_objects = extract_ean(input_url)
    
try:
    # we manage to read EAN
    ean = decoded_objects[0].data.decode("utf-8")
    st.markdown(f'🎉 __EAN détecté__: <span style="color:Red">{ean}</span>', unsafe_allow_html=True)
    subset = load_data(ean)
    # visualise product
    st.markdown(f'Consulter ce produit sur le [site `Openfoodfacts`]({subset["url"].iloc[0]})')
    st.image(subset["image_url"].iloc[0])                
    st.dataframe(subset.loc[:, ~subset.columns.str.contains("url")])
    # put some statistics
    t = f"<div>Statistiques parmi les <span class='highlight blue'>{subset['category'].iloc[0]}<span class='bold'>COICOP</span>"                
    st.markdown(t, unsafe_allow_html=True)
    # images
    for var in options:
        fig = plot_product_info(subset, var, stats_notes)
        st.plotly_chart(fig, height=800, use_container_width=True)
except:
    # we don't
    st.write('🚨 Problème de lecture de la photo, essayez de mieux cibler le code-barre')
    st.image("https://i.kym-cdn.com/entries/icons/original/000/025/458/grandma.jpg")

