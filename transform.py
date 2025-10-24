import requests
import gzip
from dotenv import load_dotenv
import os
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine



def transform_download(latest_file):
    #load_dotenv()
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD")


    engine = create_engine(
        f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    )

    existing_data = pd.read_sql("SELECT company, views, time, date FROM company_views", con=engine)

    getdate=datetime.now()
    get_date=getdate.date()
    companies = ["Amazon_(company)", "Apple_Inc.", "Facebook", "Google", "Microsoft"]
    data = []
    with gzip.open(latest_file, "rt", encoding="utf-8") as f:
        for line in f:
            parts = line.split(" ")
            if len(parts) < 3:
                continue
            title, views = parts[1], int(parts[2])
            if title in companies:
                data.append({"company": title, "views": views})
    
    df = pd.DataFrame(data)
    processed_views=df.groupby("company", as_index=False)["views"].sum()

     # Extract time safely
    try:
        time_convert = latest_file.split("-")[2].split(".")[0]
        file_time = datetime.strptime(time_convert, "%H%M%S").strftime("%H:%M:%S")
    except Exception:
        file_time = datetime.now().strftime("%H:%M:%S")

    processed_views["time"] = file_time
    processed_views["date"] = get_date

    # ✅ Force lowercase column names and correct order
    processed_views.columns = processed_views.columns.str.lower()
    for col in ["time", "date"]:
        if col not in processed_views.columns:
            processed_views[col] = None

    processed_views = processed_views[["company", "views", "time", "date"]]

    # Combine and drop duplicates
    combined_data = pd.concat([existing_data, processed_views], ignore_index=True)
    combined_data = combined_data.drop_duplicates(subset=["company", "views", "time", "date"], keep="last")

    return combined_data




    
    """  time_convert=latest_file.split("-")[2].split(".")[0]
        processed_views['Time']=datetime.strptime(time_convert, "%H%M%S").strftime("%H:%M:%S")
        processed_views['Date']=get_date

        #new_data = processed_views
        processed_views = processed_views[['company', 'views', 'time', 'date']]

        combined_data = pd.concat([existing_data, processed_views], ignore_index=True)
        combined_data = combined_data.drop_duplicates(subset=['company', 'views', 'time', 'date'], keep='last')

        combined_data = pd.concat([existing_data, processed_views], ignore_index=True)
        combined_data = combined_data.drop_duplicates(subset=['company', 'views', 'time', 'date'], keep='last')



        return combined_data """
