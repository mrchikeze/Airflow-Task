from extract import download
from transform import transform_download
from load import load_data

def run_all_scripts():
    latest_file=download()


    processed_views=transform_download(latest_file)


    load_data(processed_views)

if __name__ == "__main__":
    run_all_scripts()