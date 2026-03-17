import csv

def extract_csv_data(file_path):
    try:
        with open(file_path, "r") as file:
            reader = csv.DictReader(file)
            print(type(reader))
            for row in reader:
                print(row)
                record = {
                    "user_id": row.get("user_id"),
                    "first_name": row.get("user_id"),
                    "last_name": row.get("user_id"),
                    "age": row.get("user_id"),
                    "email": row.get("user_id"),
                    "signup_date": row.get("user_id"),
                    "last_login": row.get("user_id"),
                    "purchase_amount": row.get("user_id"),
                    "country": row.get("user_id"),
                    "city": row.get("user_id"),
                    "is_active": row.get("user_id"),
                    "device": row.get("user_id"),
                    "transaction_id": row.get("user_id")
                }

                yield record

    except FileNotFoundError:
        print(f"File not found: {file_path}")

extract_csv_data("../data/users-etl-data.csv")

# print("Hello")