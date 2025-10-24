import requests
import gzip
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd


def download():

    getdate=datetime.now()
    get_month=getdate.month
    get_year=getdate.year
    get_date=getdate.date()
    base_url = f"https://dumps.wikimedia.org/other/pageviews/{get_year}/{get_year}-{get_month}/"


    response = requests.get(base_url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a") if a["href"].endswith(".gz")]

    links.sort()
    latest_file = links[-1]
    latest_url = base_url + latest_file

    r = requests.get(latest_url, stream=True)
    with open(latest_file, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

    return latest_file