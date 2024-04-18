import random
from datetime import datetime
import psycopg2

def db_connection():
    connection = psycopg2.connect(
        database="railway", 
        user="postgres", 
        password="mqoaIaeeKncWwPQCTrTofdoWYhRXlVYk", 
        host="viaduct.proxy.rlwy.net", 
        port=24748,
        sslmode='require'
    )

    cursor = connection.cursor()

    return cursor

def get_store(db):

    db.execute("SELECT id FROM api_store ORDER BY RANDOM() LIMIT 1;")
    record = db.fetchall()
    return record

def get_department(db, store):

    db.execute(str("SELECT id FROM api_department WHERE store_id = '" + store + "' ORDER BY RANDOM() LIMIT 1;"))
    record = db.fetchall()
    return record

def get_categories(db, department):

    db.execute(str("SELECT name FROM api_category WHERE department_id = '" + department + "' ORDER BY RANDOM() LIMIT 2;"))
    record = db.fetchall()
    return record

def main():

    db = db_connection()

    age_bracket = [
        "0-15",
        "16-21",
        "22-28",
        "29-35",
        "36-45",
        "46-60",
        "61-75",
        "76+"
    ]

    gender = ["Female", "Male", "N/A"]

    store = get_store(db)

    print(store[0][0])

    department = get_department(db, store[0][0])

    print(department[0][0])

    categories = get_categories(db, department[0][0])

    print(categories[0][0])

    cat_arr = []
    for category in categories:
        cat_arr.append({
            category: random.randint(0,100)
        })

    base_json = {
        "clientType": "Retail",
        "storeId": store[0][0],
        "entryCount": random.randint(0,100),
        "exitCount": random.randint(0,100),
        "timestamp": datetime.now(),
        "persons": [
            {
                "gender": random.choice(gender),
                "ageBracket": random.choice(age_bracket),
                "firstSeenTimeStamp": datetime.now(),
                department[0][0] : cat_arr
            }
        ]
    }

    return base_json

if __name__ == "__main__":
    main()
