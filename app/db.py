import pandas as pd

import os

# DB ORM that can be used with multiple DB systems, can create tables
from sqlalchemy import create_engine

# PostgreSQL specific DB driver for python, useful for raw queries
import psycopg


class DB:
    def __init__(self):
        self.conn = None;
        self.DB_URI = os.getenv('DB_URI');
        self.dataset_path = os.getenv('DATASET_PATH');
        

    def connect(self):
        try:            
            self.conn = psycopg.connect(conninfo=self.DB_URI);
            print('Connected to PostgreSQL.');
        
        except (Exception, psycopg.DatabaseError) as error:
            print(error);
    

    def disconnect(self):
        if self.conn is not None:
            self.conn.close();
            print("DB connection closed.");
            
            
    def createTable(self):
        dataset = self.dataset_path;
        df = pd.read_csv(dataset);
        
        df.dropna(how='all');

        df.columns = df.columns.str.lower();
        df.columns = df.columns.str.strip();
        df.columns = df.columns.str.replace(' ', '_');
        df.columns = df.columns.str.replace('/', '_');
        df.columns = df.columns.str.replace('%', 'pct');
        df.columns = df.columns.str.replace('(', '');
        df.columns = df.columns.str.replace(')', '');
        
        # Load the data into a new table in the DB
        try:
            engine = create_engine(self.DB_URI);
            df.to_sql(name='disease_diagnosis', con=engine, index=False, if_exists='fail');
            print('New table created!')
                
        except(ValueError) as e:
            print(e)