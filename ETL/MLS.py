from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import logging
import pandas as pd
import json
import call_db

credentials_path = '../ETL/credentials_module.json'

# INICIAR SESION
def login():
    GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = credentials_path
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(credentials_path)
    
    if gauth.credentials is None:
        logging.info("Initial authentication")
        gauth.LocalWebserverAuth(port_numbers=[8092])
    elif gauth.access_token_expired:
        logging.info("Refreshing authentication..")
        gauth.Refresh()
    else:
        logging.info("Authentication already exists")
        gauth.Authorize()
        
    gauth.SaveCredentialsFile(credentials_path)
    credenciales = GoogleDrive(gauth)
    return credenciales

# SUBIR UN ARCHIVO A DRIVE
def upload_csv(file_path, id_folder):
    credenciales = login()
    file_csv = credenciales.CreateFile({'parents': [{"kind": "drive#fileLink",\
                                                    "id": id_folder}]})
    file_csv['title'] = file_path.split("/")[-1]
    file_csv.SetContentFile(file_path)
    file_csv.Upload()
    logging.info("Data Successfully Uploaded to Google Drive.")

def decade(year):
    """Calcula la década de un año dado."""
    return (year // 10) * 10

# COMBINACIÓN DE CSV Y DB
def merge(**kwargs):
    ti = kwargs['ti']
    def data_normalize(task_id):
        merge_data = json.loads(ti.xcom_pull(task_ids=task_id))
        return pd.json_normalize(merge_data)
    logging.info("Data merging process started...")

    grammy_df = data_normalize('T_Grammy')
    spotify_df = data_normalize('T_Spotify')

    merged_data = pd.merge(grammy_df, spotify_df, left_on='artist', right_on='artists', how='inner')
    merged_data['decade'] = merged_data['year'].apply(decade)
    if 'grammy_id' in merged_data.columns:
        merged_data.drop_duplicates(subset='grammy_id', inplace=True)
    columns_of_interest = ['year', 'category', 'nominee', 'artist', 'was_nominated', 
                           'track_id', 'artists', 'track_name', 'popularity', 'danceability', 
                           'energy', 'valence', 'album_name', 'explicit', 'decade']
    merged_data = merged_data[columns_of_interest]
    logging.info("Data merging process successfully completed.")

    return merged_data.to_json(orient='records')


# CARGAR CSV FUSIONADO A LA DB
def load(**kwargs):
    ti = kwargs["ti"]
    json_data = ti.xcom_pull(task_ids="merge")
    if json_data:
        data = pd.json_normalize(json_data)
        logging.info("Data normalized and DataFrame created.")
        
        try:
            call_db.insert_data(data)
            logging.info("Data has been successfully loaded into the database.")
        except Exception as e:
            logging.error(f"Error loading data: {str(e)}")
    else:
        logging.error("No data available to load.")


# CARGAR CSV FUSIONADO A DRIVE
def store(**kwargs):
    ti = kwargs["ti"]
    json_data = ti.xcom_pull(task_ids="merge")
    if json_data:
        data = pd.json_normalize(json_data)
        csv_file_path = './DatasetS/awards.csv'
        data.to_csv(csv_file_path, index=False)
        upload_csv(csv_file_path, '1PnHh7eQz-aWPwuXALNQ2Hu7JaGIWaYUB')
        logging.info("File 'awards.csv' stored and uploaded to Google Drive.")