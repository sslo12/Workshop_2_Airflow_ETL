import configparser
import pymysql
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

def create_connection():
    config = configparser.ConfigParser()
    config.read('./Config._db/config.ini')
    host = config['mysql']['host']
    user = config['mysql']['user']
    password = config['mysql']['password']
    database = config['mysql']['database']
    try:
        conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database)
        print("Successful Connection")
        return conn
    except pymysql.Error as e:
        print("Connection Error: %s", e)
        return None

def query_db():
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        db_table = 'SELECT * FROM grammy'
        cursor.execute(db_table)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        conn.close()
        return df

def create_table_db(cursor):
    cursor.execute("USE grammydb")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS awards (
        year INT NOT NULL,
        category VARCHAR(255),
        nominee VARCHAR(255),
        artist VARCHAR(255),
        was_nominated BOOLEAN,
        track_id VARCHAR(255),
        artists VARCHAR(255),
        track_name VARCHAR(255),
        popularity INT,
        danceability FLOAT,
        energy FLOAT,
        valence FLOAT,
        album_name VARCHAR(255),
        explicit BOOLEAN,
        decade INT)""")
    print("Table created successfully")

def insert_data(json_data):
    conn = create_connection()
    if conn is not None:
        df = pd.read_json(json_data)
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO awards (year, category, nominee, artist, was_nominated, track_id, artists, 
        track_name, popularity, danceability, energy, valence, album_name, explicit, decade)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            data_tuples = [tuple(x) for x in df.to_numpy()]
            cursor.executemany(insert_query, data_tuples)
            conn.commit()
            print("Data inserted successfully")
        except pymysql.Error as e:
            print("Failed to insert data: %s", e)
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

create_connection()