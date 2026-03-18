import logging
from itertools import tee

from scripts.Extract import extract_csv_data
from scripts.Transform import clean_data
from scripts.Load import save_cleaned_csv, load_into_database

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

    print("Saving Cleaned CSV...")

    save_cleaned_csv(csv_stream, "cleaned/cleaned_data.csv")

    print("Loading into Database...")

    load_into_database(db_stream)

    print("Pipeline completed successfully...")

if __name__ == "__main__":
    run_pipeline()