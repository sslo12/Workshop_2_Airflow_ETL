import pandas as pd
import logging
import call_db
import json

def load_db():
    logging.info("Loading data from MySQL database...")
    data_grammy = call_db.query_db()
    logging.info("Data loaded successfully.")
    return data_grammy

def transform_db(**kwargs):
    ti = kwargs["ti"]
    df_db = json.loads(ti.xcon_pull(taks_ids="load_db"))
    df = pd.json_normalize(df_db)
    logging.info("Starting cleaning and transformation processes...")
    df['published_at'] = pd.to_datetime(df['published_at'])
    df['updated_at'] = pd.to_datetime(df['updated_at'])
    logging.info("Converted dates to datetime.")
    df['nominee'].fillna('Unknown', inplace=True)
    df.dropna(subset=['artist'], inplace=True)
    logging.info("Handled missing values.")
    df['artist'] = df['artist'].str.title()
    df['nominee'] = df['nominee'].str.title()
    df.rename(columns={'winner': 'was_nominated'}, inplace=True)
    df.drop(['workers', 'img'], axis=1, inplace=True)
    logging.info("Normalized text fields and delete innecesary columns.")
    df.drop_duplicates(inplace=True)
    logging.info("Removed duplicates.")
    logging.info("Cleaning and transformation processes completed.")
    return df

#def save_db(df, output_file):
#    logging.info("Saving cleaned data...")
#    df.to_csv(output_file, index=False)
#   logging.info("Data saved successfully.")
#
#def main():
#    output_file = './Datasets/transformed_grammy.csv'
#    df = load_db()
#    if df is not None:
#        transformed_df = transform_db(df)
#        save_db(transformed_df, output_file)
#        logging.info('Data transformation process completed successfully')

#if __name__ == "__main__":
#    main()