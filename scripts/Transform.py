import logging
import re
import math
from datetime import datetime

seen_records = set()
INVALID_DATA = {"", "???", "###", None, "N/A"}
EMAIL_REGEX = r"^[\w\.-]+@[\w\.-]+\.\w+$"

def is_valid_int(val):
    try:
        int(val)
        return True
    except:
        return False

def parse_timestamp(ts):
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%d-%m-%Y %I:%M %p",
        "%Y-%m-%d"
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(ts, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            continue

def concat_firstname_lastname(firstname, lastname):
    fname = "" if firstname in INVALID_DATA else firstname
    lname = "" if lastname in INVALID_DATA else lastname

    fullname = (fname + " " + lname).strip().lower()

    return fullname

def clean_email(email):
    if not email:
        logging.warning("Missing Email")
        return None
    
    email = email.strip().lower()

    if not re.match(EMAIL_REGEX, email):
        logging.warning("Invalid Email")
        return None
    
    return email
    
def clean_age(age):
    try:
        age = math.floor(age)

        if age <= 0:
            return None

        return age
    
    except:
        return None
    
def clean_amount(amount):
    try:
        amount = float(amount)

        if amount <= 0:
            return None
        
        return amount
    
    except:
        return None

def clean_record(record):
    user_id = record.get("user_id")

    if not user_id or not is_valid_int(user_id):
        logging.warning("Missing user_id")
        return (None, "invalid")
    
    fullname = concat_firstname_lastname(record.get("first_name"), record.get("last_name"))

    if not fullname:
        logging.warning("Missing or Invalid First and Last name")
        return (None, "invalid")

    age = clean_age(record.get("age"))

    if age is None:
        logging.warning("Invalid Age")
        return (None, "invalid")

    email = clean_email(record.get("email"))

    if email is None:
        return (None, "invalid")
    
    signup_date = parse_timestamp(record.get("signup_date"))

    if not signup_date:
        logging.warning("Invalid Signup Date")
        return (None, "invalid")

    last_login = parse_timestamp(record.get("last_login"))

    if not last_login:
        logging.warning("Invalid Last Login Date")
        return (None, "invalid")

    purchase_amount = clean_amount(record.get("purchase_amount"))

    if purchase_amount is None:
        logging.warning("Invalid Purchase Amount")
        return (None, "invalid")

    country = record.get("country").lower()
    country = country.lower() if country not in INVALID_DATA else "unknown"

    city = record.get("city").lower()
    city = city.lower() if city not in INVALID_DATA else "unknown"
    
    is_active = record.get("is_active")

    if is_active is None:
        logging.warning("Invalid Status")
        return (None, "invalid")
    
    device = record.get("device").lower()

    if not device:
        device = "unknown"
    
    transaction_id = record.get("transaction_id")

    if not transaction_id or not is_valid_int(transaction_id):
        logging.warning("Invalid transaction_id")
        return (None, "invalid")
    
    key = (user_id, transaction_id)

    if key in seen_records:
        logging.info("Duplicate Record Skipped")
        return (None, "duplicate")
    
    seen_records.add(key)

    cleaned_record = {
        "user_id": user_id,
        "name": fullname,
        "age": age,
        "email": email,
        "signup_date": signup_date,
        "last_login": last_login,
        "purchase_amount": purchase_amount,
        "country": country,
        "city": city,
        "is_active": is_active,
        "device": device,
        "transaction_id": transaction_id
    }

    return (cleaned_record, "clean")

def clean_data(records):
    total_records_count = 0
    clean_records_count = 0
    invalid_records_count = 0
    duplicate_records_count = 0

    for record in records:
        
        total_records_count += 1

        cleaned, status = clean_record(record)

        if status == "clean":
            clean_records_count += 1
            yield cleaned

        elif status == "invalid":
            invalid_records_count += 1

        else:
            duplicate_records_count += 1

    print(f"Total raw records: {total_records_count}")
    print(f"Invalid records: {invalid_records_count}")
    print(f"Duplicate records: {duplicate_records_count}")
    print(f"Cleaned records: {clean_records_count}")