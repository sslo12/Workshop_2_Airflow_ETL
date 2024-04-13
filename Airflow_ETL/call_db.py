import configparser
import pymysql
import pandas as pd

def create_connection():
    config = configparser.ConfigParser()
    config.read('./config_db/config.ini')
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
    db_table = ('SELECT * FROM grammy')
    cursor.execute(db_table)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    conn.close()
    return df