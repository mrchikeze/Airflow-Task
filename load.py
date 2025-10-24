import pandas as pd
import os
import psycopg2
from psycopg2 import errors
from sqlalchemy import create_engine
from google.cloud import storage
import io

def load_data(processed_views):
    # Environment variables
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD")
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    bucket_name = os.getenv("GCP_BUCKET")

    if not credentials_path or not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Credential file not found: {credentials_path}")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    print(f"✅ Using credentials from: {credentials_path}")

    dataset_id = os.getenv("BIGQUERY_DATASET")
    table_id = os.getenv("BIGQUERY_TABLE")

    # Create PostgreSQL database if not exists
    conn = psycopg2.connect(host=db_host, port=db_port, dbname="postgres",
                            user=db_user, password=db_pass)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("CREATE DATABASE big5_views_db;")
        print("✅ Database 'big5_views_db' created.")
    except errors.DuplicateDatabase:
        print("ℹ️ Database 'big5_views_db' already exists.")
    cur.close()
    conn.close()

    # Connect to target database
    conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name,
                            user=db_user, password=db_pass)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS company_views (
            view_id SERIAL PRIMARY KEY,
            company VARCHAR(50) NOT NULL,
            views INTEGER,
            time TIME,
            date DATE
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

    # Load into Postgres
    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
    processed_views.rename(columns={'Time': 'time', 'Date': 'date'}, inplace=True)
    processed_views.to_sql(name="company_views", con=engine, if_exists="append", index=False)
    print("✅ Data loaded successfully into 'company_views' table!")


    destination_blob = "processed_company_views_test.csv"

    if not credentials_path or not bucket_name:
        raise ValueError("Set GOOGLE_APPLICATION_CREDENTIALS and GCP_BUCKET in your .env")

    client = client = storage.Client.from_service_account_json(credentials_path)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)

    if blob.exists():
        # Download existing CSV from GCS
        csv_bytes = blob.download_as_bytes()
        existing_df = pd.read_csv(io.BytesIO(csv_bytes))

        # Ensure unique columns
        existing_df = existing_df.loc[:, ~existing_df.columns.duplicated()]
        processed_views = processed_views.loc[:, ~processed_views.columns.duplicated()]

        # Reset indices
        existing_df = existing_df.reset_index(drop=True)
        processed_views = processed_views.reset_index(drop=True)

        # Align columns (intersection only)
        common_cols = existing_df.columns.intersection(processed_views.columns)
        combined_df = pd.concat([existing_df[common_cols], processed_views[common_cols]], ignore_index=True)

        # Drop duplicates based on all columns
        combined_df.drop_duplicates(inplace=True)
    else:
        combined_df = processed_views.loc[:, ~processed_views.columns.duplicated()].reset_index(drop=True)

    # Upload combined CSV back to GCS
    csv_buffer = io.StringIO()
    combined_df.to_csv(csv_buffer, index=False)
    blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

    print(f"✅ Uploaded to gs://{bucket_name}/{destination_blob} with {len(combined_df)} rows")