from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import logging

credentials_path = '../Airflow_ETL/credentials_module.json'

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

def merge(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="transform_db"))
    grammy_df = pd.json_normalize(data=json_data)

    json_data = json.loads(ti.xcom_pull(task_ids="transform_csv"))
    spotify_df = pd.json_normalize(data=json_data)

    merge_df = spotify_df.merge(grammy_df, how="inner", left_on='track_name', right_on='nominee')
    merge_df = merge_df.drop_duplicates(subset=['grammy_id'])
    merge_df = merge_df.drop(['artists', 'nominee'], axis=1)
    merge_df['decade'] = merge_df['year'].apply(classify_decade)

    logging.info("Se ha realizado la fusión de datos con éxito.")
    return merge_df.to_json(orient='records')

def load(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="merge"))
    data = pd.json_normalize(data=json_data)

    logging.info("Cargando datos...")
    
    db_operations.insert_data(data)
    
    logging.info("Los datos se han cargado en: tracks")

def store(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="merge"))
    data = pd.json_normalize(data=json_data)
    data.to_csv('./dataset/data.csv')

    upload_file('./dataset/data.csv', '1KPtdh_iCBpPc_6WDjKLNetMzjGsYnDi-')
    logging.info("Archivo 'data.csv' almacenado y subido a Google Drive.")