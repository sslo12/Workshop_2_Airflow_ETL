from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import logging
import json
import pandas as pd
import call_db
from T_Grammy import transform_db
from T_Spotify import load_csv, transform_csv

credentials_path = 'Airflow_ETL/credentials_module.json'

# LOGIN SESSION
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
    drive = GoogleDrive(gauth)
    return drive

# UPLOAD A FILE TO DRIVE
def upload_csv(file_path, id_folder):
    drive = login()
    file_csv = drive.CreateFile({'parents': [{"kind": "drive#fileLink", "id": id_folder}]})
    file_csv['title'] = file_path.split("/")[-1]
    file_csv.SetContentFile(file_path)
    file_csv.Upload()
    logging.info(f"Data Successfully Uploaded to Google Drive: {file_path}")

# COMBINE CSV AND DB DATA
def merge(spotify_data, grammy_data):
    grammy = json.loads(grammy_data)
    grammy_df = pd.json_normalize(grammy)
    spotify = json.loads(spotify_data)
    spotify_df = pd.json_normalize(spotify)
    logging.info("Data merging process started...")

    merged_df = spotify_df.merge(grammy_df, how="inner", left_on='track_name', right_on='nominee')
    merged_df['nomination_dec'] = merged_df['year'].apply(lambda x: (x // 10) * 10)
    merged_df.drop(['artists', 'nominee'], axis=1, inplace=True)
    col_interest = ['year', 'category', 'was_nominated', 'artist',
                    'track_name', 'popularity', 'danceability', 
                    'energy', 'explicit', 'nomination_dec']
    merged_data = merged_df[col_interest]
    logging.info("Data merging process successfully completed.")
    
    folder_path = './Datasets'
    file_name = 'awards.csv'
    file_path = f"{folder_path}/{file_name}"
    merged_data.to_csv(file_path, index=False)
    logging.info("Data merging save in folder Datasets!!")
    return merged_data.to_json(orient='records')

# LOAD DATA TO DATABASE
def load_to_db(data):
    data = json.loads(data)
    data_load = pd.json_normalize(data)
    call_db.insert_data(data_load)
    logging.info("Data has been successfully loaded into the database.")


# STORE AND UPLOAD DATA TO DRIVE
def store_to_drive():
    file_path = './Datasets/awards.csv'
    upload_csv(file_path, '1PnHh7eQz-aWPwuXALNQ2Hu7JaGIWaYUB')
    logging.info("File 'awards.csv' stored and uploaded to Google Drive.")

# MAIN EXECUTION
def main():
    df_spotify = load_csv()
    df_spotify_transform = transform_csv(df_spotify)
    df_grammy_transform = transform_db()

    merged_data = merge(df_spotify_transform, df_grammy_transform)
    load_to_db(merged_data)
    store_to_drive()

if __name__ == "__main__":
    main()