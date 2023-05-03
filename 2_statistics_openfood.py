import pandas as pd
import s3fs


ENDPOINT_URL = "https://minio.lab.sspcloud.fr"
BUCKET = "projet-funathon"
URL = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
filename = URL.rsplit("/", maxsplit=1)[-1]
DESTINATION_RAW = f"{BUCKET}/2023/sujet4/diffusion"

fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL})


# methode 1
with fs.open(f"{DESTINATION_RAW}/openfood.parquet", "rb") as remote_file:
    openfood = pd.read_parquet(remote_file)

