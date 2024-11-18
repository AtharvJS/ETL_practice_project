import pandas as pd 
import requests
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np

url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
temp_table_attribs = ['Name','MC_USD_Billion']
final_table_attribs = ['Name','MC_USD_Billion','MC_GBP_Billion','MC_EUR_Billion','MC_INR_Billion']
csv_path = './Largest_banks_data.csv'
sql_connection = sqlite3.connect('Banks.db')
table_name = 'Largest_banks'
log_file = './code_log.txt'

def log_progress(message):
    timestamp_format = '%d/%h/%Y—%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ' — ' + message + '\n')

def extract(url, temp_table_attribs):
    df = pd.DataFrame(columns=temp_table_attribs)
    r = requests.get(url)
    s = BeautifulSoup(r.content, 'html.parser')
    table = s.find('tbody')
    rows_temp = table.find_all('tr')
    rows = rows_temp[1:]
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {'Name': col[1].text.strip(),
                        'MC_USD_Billion': col[2].text.strip()}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
        else:
            break
    return df

def transform(df):
    df['MC_USD_Billion'] = df['MC_USD_Billion'].str.replace(',','')
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)
    df['MC_GBP_Billion'] = [np.round(x*0.8,2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*0.93,2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*82.95,2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_queries(query, sql_connection):
    output = pd.read_sql(query, sql_connection)
    print(output)


# Process Execution
log_progress('ETL Process started')

# Extract Phase
log_progress('Extract phase started')
df = extract(url,temp_table_attribs)
log_progress('Extract phase completed')

#Transform phase
log_progress('Transform phase started')
df = transform(df)
log_progress('Transform phase completed')

# Load phase
log_progress('Load phase started')
load_to_csv(df, csv_path)
log_progress('Data loaded to .csv file')
load_to_db(df, sql_connection)
log_progress('Data loaded to database')

print('Query #1')
query = 'SELECT * FROM Largest_banks'
run_queries(query, sql_connection)

print('Query #2')
query = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_queries(query, sql_connection)

print('Query #3')
query = 'SELECT Name from Largest_banks LIMIT 5'
run_queries(query, sql_connection)

log_progress('ETL Process completed successfully')