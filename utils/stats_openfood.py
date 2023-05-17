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