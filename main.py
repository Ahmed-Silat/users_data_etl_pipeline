import logging

from scripts.Extract import extract_csv_data

logging.basicConfig(
    filename = "logs/pipeline.log",
    level = logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s"
)

def run_pipeline():

    print("Reading Data From The CSV...")

    csv_data = extract_csv_data("data/users-etl-data.csv")

    print("Cleaning Data...")

if __name__ == "__main__":
    run_pipeline()