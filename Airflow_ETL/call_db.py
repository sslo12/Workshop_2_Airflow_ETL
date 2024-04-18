import configparser
import pymysql
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

 
def create_connection():
    config = configparser.ConfigParser()
    config.read('./Config_db/config.ini')
    host = config['mysql']['host']
    user = config['mysql']['user']
    password = config['mysql']['password']
    database = config['mysql']['database']

    try:
        conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        print("Successful Connection")
        cursor = conn.cursor()
        return conn, cursor
    except pymysql.Error as e:
        print("Connection Error:", e)
    return None, None

def query_db():
    conn, cursor = create_connection()
    if conn and cursor:
        try:
            query = 'SELECT * FROM grammy'
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            return df
        except pymysql.Error as e:
            logging.error(f"Error fetching data: {e}")
        finally:
            conn.close()
            logging.info("Database connection closed.")
    else:
        logging.info("No database connection available.")
        return None

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
    conn, cursor = create_connection()
    if conn is not None and cursor is not None:
        try:
            df = pd.read_json(json_data)
            insert_query = """
            INSERT INTO awards (
                year, category, nominee, artist, was_nominated, track_id, artists,
                track_name, popularity, danceability, energy, valence, album_name, explicit, decade
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            data_tuples = [tuple(x) for x in df.to_numpy()]
            cursor.executemany(insert_query, data_tuples)
            conn.commit()
            logging.info("Data inserted successfully")
        except pymysql.Error as e:
            logging.error("Failed to insert data: %s", e)
            conn.rollback()
        except ValueError as e:
            logging.error("Data processing error: %s", e)
        finally:
            cursor.close()
            conn.close()
    else:
        logging.error("Connection failed")