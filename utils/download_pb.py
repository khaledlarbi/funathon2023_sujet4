"""
Some useful functions
"""
import os
import requests
from tqdm import tqdm
import pandas as pd

def download_pb(
    url: str, fname: str, total: int = None, force: bool = True, verify: bool = True
):
    """Useful function to get request with a progress bar
    Borrowed from
    https://gist.github.com/yanqd0/c13ed29e29432e3cf3e7c38467f42f51
    Arguments:
        url {str} -- URL for the source file
        fname {str} -- Destination where data will be written
        total {int} -- Filesize. Optional argument, recommended to let the default value.
        verify {bool} -- Optional argument, inherited from requests.get
    """

    try:
        proxies = {"http": os.environ["http_proxy"], "https": os.environ["https_proxy"]}
    except KeyError:
        proxies = {"http": "", "https": ""}

    resp = requests.get(url, proxies=proxies, stream=True, verify=verify, timeout=600)

    if total is None and force is False:
        total = int(resp.headers.get("content-length", 0))

    with open(fname, "wb") as file, tqdm(
        desc="Downloading: ",
        total=total,
        unit="iB",
        unit_scale=True,
        unit_divisor=1024,
    ) as obj:
        for data in resp.iter_content(chunk_size=1024):
            size = file.write(data)
            obj.update(size)


def import_coicop_labels(url: str) -> pd.DataFrame:
    coicop = pd.read_excel(url, skiprows=1)
    coicop['Code'] = coicop['Code'].str.replace("'", "")
    coicop = coicop.rename({"Libellé": "category"}, axis = "columns")
    return coicop
