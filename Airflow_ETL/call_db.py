import configparser
import pymysql
import pandas as pd
import logging

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
    query = 'SELECT * FROM grammy'
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    df.rename(columns=dict(zip(range(len(columns)), columns)), inplace=True)
    conn.close()
    return df

def create_table_db(cursor):
    cursor.execute("USE grammydb")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS awards (          
        year INT NOT NULL,
        category VARCHAR(255),
        was_nominated BOOLEAN,
        artist VARCHAR(255),
        track_name VARCHAR(255),
        popularity INT,
        danceability FLOAT,
        energy FLOAT,
        explicit BOOLEAN,
        nomination_dec INT)""")
    logging.info("Table created successfully")

def insert_data(j_data):
    conn, cursor = create_connection()
    create_table_db(cursor)
    df = pd.DataFrame(j_data)

    col_interest = ['year', 'category', 'was_nominated', 'artist', 'track_name', 
                    'popularity', 'danceability', 'energy', 'explicit', 'nomination_dec']
    df = df[col_interest]
    insert_query = f"""
        INSERT INTO awards({", ".join(col_interest)})
        VALUES ({", ".join(["%s"] * len(col_interest))})
    """
    for _, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))
    conn.commit()
    conn.close()
    logging.info("All data inserted successfully.")


#def insert_data(json_data):
#    conn, cursor = create_connection()
#    conn, cursor = create_table_db()
#    df = pd.read_json(json_data)
#    insert_query = """
#    INSERT INTO awards (
#        year, category, nominee, artist, was_nominated, track_id, artists,
#        track_name, popularity, danceability, energy, valence, album_name, explicit, decade
#    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#    """
#    data_tuples = [tuple(x) for x in df.to_numpy()]
#    cursor.executemany(insert_query, data_tuples)
#    conn.commit()
#    logging.info("Data inserted successfully") 
#    cursor.close()
#    conn.close()
