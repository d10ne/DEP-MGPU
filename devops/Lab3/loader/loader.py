import pandas as pd
import psycopg2
import os

# подключение к БД
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

cur = conn.cursor()

df = pd.read_csv("/data/uber-raw-data-apr14.csv")

df["Date/Time"] = pd.to_datetime(df["Date/Time"], format="%m/%d/%Y %H:%M:%S")

cur.execute("""
CREATE TABLE IF NOT EXISTS trips (
    id SERIAL PRIMARY KEY,
    datetime TIMESTAMP,
    lat FLOAT,
    lon FLOAT,
    base TEXT
)
""")

for _, row in df.iterrows():
    cur.execute(
        "INSERT INTO trips (datetime, lat, lon, base) VALUES (%s,%s,%s,%s)",
        (row["Date/Time"], row["Lat"], row["Lon"], row["Base"])
    )

conn.commit()

cur.close()
conn.close()

print("CSV успешно загружен в PostgreSQL")