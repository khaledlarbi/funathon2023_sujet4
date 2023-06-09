import pandas as pd

def compute_stats_nutriments(data):
    stats_nutritionnelles = (
        data
        .groupby("coicop")
        .quantile([i/10 for i in range(1,10)])
        .reset_index(names=['coicop', 'quantile'])
    )
    stats_nutritionnelles['quantile'] = stats_nutritionnelles['quantile'].mul(10).astype(int)
    stats_nutritionnelles = pd.melt(stats_nutritionnelles, id_vars=['coicop', 'quantile'])
    return stats_nutritionnelles

def compute_stats_grades(data, indices_synthetiques):
    stats_notes = (
        data
        .groupby("coicop")
        .agg({i:'value_counts' for i in indices_synthetiques})
        .reset_index(names=['coicop', 'note'])
    )
    stats_notes = pd.melt(stats_notes, id_vars = ['coicop','note'])
    stats_notes = stats_notes.dropna().drop_duplicates(subset = ['variable','note','coicop'])
  
    return stats_notes


def quantile_one_variable_sql(con, variable="energy-kcal_100g", path_within_s3 = "temp.parquet"):
    query = "SELECT coicop, " + ", ".join(
        [f"PERCENTILE_CONT({q/10}) WITHIN GROUP (ORDER BY \"{variable}\") AS quantile{q}" for q in range(1,10)]
    ) + f" FROM read_parquet('s3://{path_within_s3}') GROUP BY coicop"
    quantile_one_variable = pd.melt(con.sql(query).df(), id_vars="coicop", var_name="quantile")
    quantile_one_variable['quantile'] = quantile_one_variable['quantile'].str.replace("quantile","").astype(int)
    quantile_one_variable['variable'] = variable
    return quantile_one_variable


def count_one_variable_sql(con, variable, path_within_s3 = "temp.parquet"):
    query = f"SELECT coicop, {variable} AS note, COUNT({variable}) AS value FROM read_parquet('s3://{path_within_s3}') GROUP BY coicop, {variable}"
    stats_one_variable = con.sql(query).df().dropna()
    stats_one_variable['variable'] = variable
    stats_one_variable = stats_one_variable.replace('', 'NONE')

    return stats_one_variable