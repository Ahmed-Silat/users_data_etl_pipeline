import csv

def extract_csv_data(file_path):
    try:
        with open(file_path, "r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            # print(type(reader))
            for row in reader:
                # print(row)
                record = {
                    "user_id": row.get("user_id"),
                    "first_name": row.get("first_name"),
                    "last_name": row.get("last_name"),
                    "age": row.get("age"),
                    "email": row.get("email"),
                    "signup_date": row.get("signup_date"),
                    "last_login": row.get("last_login"),
                    "purchase_amount": row.get("purchase_amount"),
                    "country": row.get("country"),
                    "city": row.get("city"),
                    "is_active": row.get("is_active"),
                    "device": row.get("device"),
                    "transaction_id": row.get("transaction_id")
                }

                yield record

    except FileNotFoundError:
        print(f"File not found: {file_path}")

extract_csv_data("../data/users-etl-data.csv")
