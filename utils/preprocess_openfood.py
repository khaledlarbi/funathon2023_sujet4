import os
import numpy as np
import s3fs
import pandas as pd
import fasttext

from utils.download_pb import download_pb
from utils.import_yaml import import_yaml
from utils.utils_ddc import preprocess_text

config = import_yaml("config.yaml")

DESTINATION_RAW = f"{config['BUCKET']}/{config['DESTINATION_DATA_S3']}"
DESTINATION_OPENFOOD = f"{DESTINATION_RAW}/openfood.parquet"

fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": config["ENDPOINT_S3"]}
)


# OPENFOOD -------------------------------------

def download_openfood(
        url: str = config["URL_OPENFOOD"],
        destination: str = "openfood.csv",
        force: bool = False
    ):
    
    if os.path.exists(destination) is True and force is False:
        print(f"{destination} exists, if you want to override, add argument force=True")
        return None
    
    download_pb(url, destination)

    
def import_openfood(filename):
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
    
    return openfood


def clean_column_dataset(
    data: pd.DataFrame,
    dict_rules_replacement: dict,
    variable_to_clean: str, variable_output: str) :
    
    data[variable_output] = data[variable_to_clean].str.upper()
    data = data.replace({variable_output: dict_rules_replacement}, regex=True)
    return data


def import_coicop_labels(url: str) -> pd.DataFrame:
    coicop = pd.read_excel(url, skiprows=1)
    coicop['Code'] = coicop['Code'].str.replace("'", "")
    coicop = coicop.rename({"Libellé": "category"}, axis = "columns")
    return coicop


def model_predict_coicop(data, model, product_column: str = "preprocessed_labels", output_column: str = "coicop"):
    predictions = pd.DataFrame(
        {
        output_column: \
            [k[0] for k in model.predict(
                [str(libel) for libel in data[product_column]], k = 1
                )[0]]
        })

    data[output_column] = predictions[output_column].str.replace(r'__label__', '')
    return data


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


def fuzzy_matching_product(product_name, con, url_data, liste_colonnes_sql, info_nutritionnelles, indices_synthetiques):
    out_textual = con.sql(f"SELECT {liste_colonnes_sql} from read_parquet('{url_data}') WHERE jaro_winkler_similarity('{product_name}',product_name) > 0.8 AND \"energy-kcal_100g\" IS NOT NULL")
    out_textual = out_textual.df()

    out_textual.loc[:, indices_synthetiques] = pd.concat(
        [clean_note(out_textual, s, "wide") for s in indices_synthetiques],
        axis = 1
    )

    out_textual_imputed = pd.concat(
        [
            openfood_produit.loc[:, ["code", "product_name", "coicop"]].reset_index(drop = True),
            pd.DataFrame(out_textual.loc[:, info_nutritionnelles  ].mean()).T,
            pd.DataFrame(out_textual.loc[:, indices_synthetiques].replace('',np.nan).mode())
        ], ignore_index=True, axis=1
    )
    out_textual_imputed.columns = ["code", "product_name", "coicop"] + info_nutritionnelles + indices_synthetiques
    
    return out_textual_imputed

