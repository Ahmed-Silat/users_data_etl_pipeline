import csv
import psycopg2
import os
from dotenv import load_dotenv

def save_cleaned_csv(records, file_path):
    if not records:
        return
    
    first_record = None

    with open(file_path, "w", newline = "") as file:
        
        writer = None

        for record in records:
            if first_record is None:
                first_record = record
                keys = record.keys()

                writer = csv.DictWriter(file, fieldnames = keys)
                writer.writeheader()
            
            writer.writerow(record)

def load_into_database(records):

    load_dotenv()

    conn = psycopg2.connect(
        host = os.getenv("DB_HOST"),
        database = os.getenv("DB_NAME"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        port = os.getenv("DB_PORT")
    )

    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users_info (
        id SERIAL PRIMARY KEY,
        user_id INTEGER,
        name TEXT,
        age INTEGER,
        email TEXT,
        signup_date DATE,
        last_login DATE,
        purchase_amount NUMERIC,
        country TEXT,
        city TEXT,
        is_active BOOLEAN,
        device TEXT,
        transaction_id INTEGER
        )""")
    
    cursor.execute("TRUNCATE TABLE users_info RESTART IDENTITY")

    values = []

    for r in records:
        values.append((
            r["user_id"],
            r["name"],
            r["age"],
            r["email"],
            r["signup_date"],
            r["last_login"],
            r["purchase_amount"],
            r["country"],
            r["city"],
            r["is_active"],
            r["device"],
            r["transaction_id"]
        ))
    
    try:
        cursor.executemany("""
            INSERT INTO users_info
            (user_id, name, age, email, signup_date, last_login, purchase_amount, country, city, is_active, device, transaction_id)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, values)
        
        conn.commit()
    
    except Exception as e:
        conn.rollback()
        print("Error inserting records:", e)
    
    cursor.close()
    conn.close()