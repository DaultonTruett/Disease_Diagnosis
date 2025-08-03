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


    def queryAll(self):
        cursor = self.conn.cursor()
        
        query = """SELECT * FROM disease_diagnosis;""";
        cursor.execute(query);
        
        df = self.convertToDataframe(cursor.fetchall(), cursor.description);
        
        cursor.close();
        
        return df;
    
    
    def querySymptoms(self):
        cursor = self.conn.cursor();
        
        query = """SELECT symptom_1, symptom_2, symptom_3,
            count(symptom_1) as symptom_1_count,
            count(symptom_2) as symptom_2_count,
            count(symptom_3) as symptom_3_count
            FROM disease_diagnosis
            GROUP BY symptom_1, symptom_2, symptom_3
            ORDER BY symptom_1, symptom_2, symptom_3 DESC
            LIMIT 10;""";

        cursor.execute(query);
        
        df = self.convertToDataframe(cursor.fetchall(), cursor.description);
        
        cursor.close();
        
        return df;


    def queryDiagnosisCount(self):
        cursor = self.conn.cursor();
        
        query = """SELECT diagnosis,
            count(diagnosis) as diagnosis_count
            FROM disease_diagnosis
            GROUP BY diagnosis;""";

        cursor.execute(query);
        
        df = self.convertToDataframe(cursor.fetchall(), cursor.description);
        
        cursor.close();
        
        return df;
    
    
    def queryAgeCount(self):
        cursor = self.conn.cursor();
        
        query = """SELECT age, count(age) 
            FROM disease_diagnosis
            GROUP BY age
            ORDER BY age;""";
            
        cursor.execute(query);
        
        df = self.convertToDataframe(cursor.fetchall(), cursor.description);
        
        cursor.close();
        
        return df;


    def convertToDataframe(self, data, description):
        return pd.DataFrame.from_records(data, columns=[name[0] for name in description]);