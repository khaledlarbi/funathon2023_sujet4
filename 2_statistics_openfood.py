import pandas as pd
import s3fs


ENDPOINT_URL = "https://minio.lab.sspcloud.fr"
BUCKET = "projet-funathon"
URL = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
filename = URL.rsplit("/", maxsplit=1)[-1]
DESTINATION_RAW = f"{BUCKET}/2023/sujet4/diffusion"

fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL})


# methode 1: pandas

with fs.open(f"{DESTINATION_RAW}/openfood.parquet", "rb") as remote_file:
    openfood = pd.read_parquet(remote_file)

info_nutritionnelles = [
    'energy-kcal_100g', 'fat_100g', 'saturated-fat_100g',
    'carbohydrates_100g', 'sugars_100g',
    'proteins_100g', 'salt_100g']

grades = ["nutriscore_grade", "ecoscore_grade", "nova_group"]


stats_nutritionnelles = openfood.loc[:,info_nutritionnelles + ["category"]].groupby("category").quantile([i/10 for i in range(1,10)]).reset_index(names=['category', 'quantile'])
stats_nutritionnelles['quantile'] = stats_nutritionnelles['quantile'].mul(10).astype(int)
stats_nutritionnelles = pd.melt(stats_nutritionnelles, id_vars=['category', 'quantile'])

stats_notes = openfood.loc[:,["category"] + grades].groupby("category").agg({i:'value_counts' for i in grades}).reset_index(names=['category', 'note'])
stats_notes = pd.melt(stats_notes, id_vars = ['category','note'])
stats_notes['note'] = stats_notes['note'].astype(str)
stats_notes['value'] = stats_notes['value'].astype("Int64")

with fs.open(f"{DESTINATION_RAW}/stats_nutritionnelles_pandas.parquet", "wb") as f:
    stats_nutritionnelles.to_parquet(f)

with fs.open(f"{DESTINATION_RAW}/stats_notes_pandas.parquet", "wb") as f:
    stats_notes.to_parquet(f)


# methode 2: duckdb
import duckdb
con = duckdb.connect(database=':memory:')
con.execute("""
INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint='minio.lab.sspcloud.fr'
""")

def quantile_one_variable_sql(variable):
    query = "SELECT category, " + ", ".join(
        [f"PERCENTILE_CONT({q/10}) WITHIN GROUP (ORDER BY \"{variable}\") AS quantile{q}" for q in range(1,10)]
    ) + " FROM read_parquet('temp.parquet') GROUP BY category"
    quantile_one_variable = pd.melt(con.sql(query).df(), id_vars="category", var_name="quantile")
    quantile_one_variable['quantile'] = quantile_one_variable['quantile'].str.replace("quantile","").astype(int)
    quantile_one_variable['variable'] = variable
    return quantile_one_variable


stats_nutritionnelles_sql = [quantile_one_variable_sql(nutriment) for nutriment in info_nutritionnelles]
stats_nutritionnelles_sql = pd.concat(stats_nutritionnelles_sql)


def count_one_variable_sql(variable):
    query = f"SELECT category, {variable} AS note, COUNT({variable}) AS value FROM read_parquet('temp.parquet') GROUP BY category, {variable}"
    stats_one_variable = con.sql(query).df().dropna()
    stats_one_variable['variable'] = variable
    return stats_one_variable


stats_notes_sql = [count_one_variable_sql(note) for note in grades]
stats_notes_sql = pd.concat(stats_notes_sql)


with fs.open(f"{DESTINATION_RAW}/stats_nutritionnelles_sql.parquet", "wb") as f:
    stats_nutritionnelles_sql.to_parquet(f)

with fs.open(f"{DESTINATION_RAW}/stats_notes_sql.parquet", "wb") as f:
    stats_notes_sql.to_parquet(f)