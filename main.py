import logging
from itertools import tee

from scripts.Extract import extract_csv_data
from scripts.Transform import clean_data

logging.basicConfig(
    filename = "logs/pipeline.log",
    level = logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s"
)

def run_pipeline():

    print("Reading Data From The CSV...")

    csv_data = extract_csv_data("data/users-etl-data.csv")

    print("Cleaning Data...")

    cleaned = clean_data(csv_data)

    csv_stream, db_stream = tee(cleaned)

if __name__ == "__main__":
    run_pipeline()