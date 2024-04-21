import pandas as pd
import numpy as np
import logging
import call_db

def load_db():
    logging.info("Loading data from MySQL database...")
    data_grammy = call_db.query_db()
    logging.info("Data loaded successfully.")
    #print("Data loaded:", data_grammy[:5])
    return data_grammy


def transform_db(**kwargs):
    ti = kwargs["ti"]
    json_data = ti.xcom_pull(task_ids="read_db")
    logging.info("Starting cleaning and transformation processes...")
    #df_db = call_db.query_db()
    df = pd.DataFrame(json_data)

    df.dropna(subset=['artist'], inplace=True)
    logging.info("Handled missing values.")

    df[['winner', 'nominee', 'artist', 'year']]
    df.rename(columns={'winner': 'was_nominated'}, inplace=True)
    logging.info("Deleted unnecessary columns.")

    df.drop_duplicates(inplace=True)
    logging.info("Removed duplicates.")
    logging.info("Cleaning and transformation processes completed.")
    return df.to_json(orient='records')

#df = load_db()
#df1 = transform_db()
#print(df1)


#def save_db(df, output_file):
#  logging.info("Saving cleaned data...")
#  df.to_csv(output_file, index=False)
#  logging.info("Data saved successfully.")

#def main():
#    output_file = './Datasets/transformed_grammy.csv'
#    df = load_db()
#    if df is not None:
#        transformed_df = transform_db(df)
#        save_db(transformed_df, output_file)
#        logging.info('Data transformation process completed successfully')

#if __name__ == "__main__":
#    main()