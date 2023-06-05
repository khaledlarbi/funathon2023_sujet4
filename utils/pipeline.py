import numpy as np
import pandas as pd
from utils.download_pb import import_coicop_labels

info_nutritionnelles = [
    'energy-kcal_100g', 'fat_100g', 'saturated-fat_100g',
    'carbohydrates_100g', 'sugars_100g',
    'proteins_100g', 'salt_100g']
indices_synthetiques = [
    "nutriscore_grade", "ecoscore_grade", "nova_group"
]
principales_infos = ['product_name', 'code', 'preprocessed_labels', 'coicop', 'url', 'image_url']
liste_colonnes = principales_infos + indices_synthetiques + info_nutritionnelles
liste_colonnes_sql = [f"\"{s}\"" for s in liste_colonnes]
liste_colonnes_sql = ', '.join(liste_colonnes_sql)

import duckdb
con = duckdb.connect(database=':memory:')
con.execute("""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_endpoint='minio.lab.sspcloud.fr'
""")

url_data = "https://projet-funathon.minio.lab.sspcloud.fr/2023/sujet4/diffusion/openfood.parquet"
coicop = import_coicop_labels(url = "https://www.insee.fr/fr/statistiques/fichier/2402696/coicop2016_liste_n5.xls")

def fuzzy_matching_product(openfood_produit, product_name, con, url_data, liste_colonnes_sql, indices_synthetiques):
    out_textual = con.sql(f"SELECT {liste_colonnes_sql} from read_parquet('{url_data}') WHERE jaro_winkler_similarity('{product_name}',product_name) > 0.8 AND \"energy-kcal_100g\" IS NOT NULL")
    out_textual = out_textual.df()

    out_textual_imputed = pd.concat(
        [
            openfood_produit.loc[:, ["code", "product_name", "coicop"]].reset_index(drop = True),
            pd.DataFrame(out_textual.loc[:, indices_synthetiques].replace("NONE","").replace('',np.nan).mode(dropna=True))
        ], ignore_index=True, axis=1
    )
    out_textual_imputed.columns = ["code", "product_name", "coicop"] + indices_synthetiques
    
    return out_textual_imputed


def find_product_openfood(con, liste_colonnes_sql, url_data, ean, coicop):
    openfood_produit = con.sql(
        f"SELECT {liste_colonnes_sql} FROM read_parquet('{url_data}') WHERE CAST(ltrim(code, '0') AS STRING) = CAST(ltrim({ean}) AS STRING)"
    ).df()
    
    product_name = openfood_produit["product_name"].iloc[0]
    
    if openfood_produit['nutriscore_grade'].isin(['NONE','']).iloc[0]:
        openfood_produit = fuzzy_matching_product(
            openfood_produit, product_name, con, url_data,
            liste_colonnes_sql, indices_synthetiques)

    openfood_produit = openfood_produit.merge(coicop, left_on = "coicop", right_on = "Code")

    return openfood_produit



def clean_note(data, yvar = "note", format = "long"):
    stats_one_variable = data.copy()
    stats_one_variable[yvar] = stats_one_variable[yvar].astype(str)
    if format == "long":
        stats_one_variable = stats_one_variable.loc[~stats_one_variable[yvar].isin(['unknown','not-applicable'])]
        stats_one_variable = stats_one_variable.dropna().drop_duplicates(subset = ['variable', yvar, 'coicop'])
        stats_one_variable.loc[stats_one_variable['variable'] == "nova_group", yvar] = (
                stats_one_variable.loc[stats_one_variable['variable'] == "nova_group", yvar]
                .astype(float).astype("int64", errors = "ignore").apply(lambda d: chr(d + 64))
            )
    if format == "wide" :
        stats_one_variable[yvar] = stats_one_variable[yvar].replace(r'(unknown|not-applicable)', '', regex = True)
        if yvar == "nova_group":
            stats_one_variable[yvar] = stats_one_variable[yvar].replace(r'nan', '', regex = True)
            stats_one_variable.loc[stats_one_variable[yvar] == "", yvar] = 0 #hack to handle nan in conversion
            stats_one_variable[yvar] = (stats_one_variable[yvar]
                                        .astype(float)
                                        .astype("int64", errors = "ignore").apply(lambda d: chr(d + 64))
                                       )



    stats_one_variable[yvar] = stats_one_variable[yvar].str.upper()
    stats_one_variable[yvar] = stats_one_variable[yvar].astype(str)
    stats_one_variable[yvar] = stats_one_variable[yvar].replace(r'NAN', '')
    stats_one_variable[yvar] = stats_one_variable[yvar].replace(r'@', '')

    if format == "wide":
        return stats_one_variable[yvar]
    return stats_one_variable

