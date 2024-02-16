from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import requests
import pandas as pd
import subprocess
import psycopg2
import json



def extract_from_api(**kwargs):
    villes = ['paris', 'bordeaux', 'marseille', 'toulouse']
    data_villes = {}

    for ville in villes:
        url = f'https://api.waqi.info/feed/{ville}/?token=your_token'

        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                data_villes[ville] = data
                print(data)

            else:
                print(f"La requête a échoué avec le code {response.status_code}")

        except Exception as e:
            print(f"Une erreur s'est produite : {e}")

    return data_villes



def transform_data(**kwargs):
    ti = kwargs['ti']
    data_villes = ti.xcom_pull(task_ids='tsk_extract_from_api')
    villes = ['paris', 'bordeaux', 'marseille', 'toulouse']
    data_villes_transformed = {}

    for ville in villes:
        iaqi_data = data_villes[ville]["data"]["iaqi"]
        df = pd.DataFrame(iaqi_data)
        df = df.rename(columns={'pm25': ' pm25', 'pm10': ' pm10', 
                                'o3': ' o3', 'no2': ' no2'})
        df = df[[' pm25', ' pm10', ' o3', ' no2']]

        date_string = data_villes[ville]["data"]["time"]["s"]
        date_string = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
        date_string = date_string.strftime('%Y/%-m/%-d')

        df['date'] = date_string

        df = df[['date', ' pm25', ' pm10', ' o3', ' no2']]

        data_villes_transformed[ville] = df


    return data_villes_transformed



def load_to_postgre(**kwargs):
    ti = kwargs['ti']
    data_villes_transformed = ti.xcom_pull(task_ids='tsk_transform_data')
    username = 'your_db_username'
    password = 'your_db_password'
    host = 'localhost'
    port = 5234
    db_name = 'your_db'

    engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{db_name}")

    for ville, df in data_villes_transformed.items():
        table_name=f'{ville}_air_quality'
        df=df.sort_values(by='date', ascending=False)
        df.to_sql(table_name,engine,index=False,if_exists='append')
        print(df)

        print(f'Data loaded to {table_name} successfully.')



def extract_data():
    bordeaux_data = pd.read_csv('/home/lifu237/bordeaux_air_quality.csv')
    paris_data = pd.read_csv('/home/lifu237/paris_air_quality.csv')
    marseille_data = pd.read_csv('/home/lifu237/marseille_air_quality.csv')
    toulouse_data = pd.read_csv('/home/lifu237/toulouse_air_quality.csv')
    # lyon_data = pd.read_csv('/home/lifu237/lyon_air_quality.csv')
    username = 'your_db_username'
    password = 'your_db_password'
    host = 'localhost'
    db_name = 'your_db'
    conn = f'postgresql://{username}:{password}@{host}:5433/{db_name}'

    engine = create_engine(conn)

    # bordeaux_data.to_sql('bordeaux_air_quality', engine, if_exists='replace', index=False)
    # paris_data.to_sql('paris_air_quality', engine, if_exists='replace', index=False)
    # marseille_data.to_sql('marseille_air_quality', engine, if_exists='replace', index=False)
    # toulouse_data.to_sql('toulouse_air_quality', engine, if_exists='replace', index=False)
    # # lyon_data.to_sql('lyon_air_quality', engine, if_exists='replace', index=False)



def from_pg_to_s3_daily():
    conn_params = {
        'user': 'your_db_username',
        'password': 'your_db_password',
        'host': 'localhost',
        'database': 'myour_db_name',
        'port':5432
    }
    conn=None
    try:
        conn = psycopg2.connect(**conn_params)
        tables = ['bordeaux_air_quality', 'paris_air_quality', 'marseille_air_quality', 'toulouse_air_quality']
        S3_BUCKET_NAME = 'mspr-epsi-raw-data-1'
        now = datetime.now().strftime("%d%m%Y%H%M%S")

        for table in tables:
            query = f"SELECT * FROM {table};"
            df = pd.read_sql(query, conn)
            file_name = f"{table}_{now}.csv"
            filepath = f'/home/lifu237/{file_name}'
            df.to_csv(filepath, index=False, sep=';')

            subprocess.run(['aws', 's3', 'cp', filepath, f's3://{S3_BUCKET_NAME}/historic/{table}/'])

        print("Les données ont été exportées depuis postgresql vers le s3 aws avec succès!")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if conn is not None:
            conn.close()




########################API WEATHER########################

def extract_data_from_weather_api():
    token='your_token'
    # units='metric'
    villes = ['paris', 'bordeaux', 'marseille', 'toulouse']
    data_extracted_from_weather_api={}
    for ville in villes:
        url=f'https://api.openweathermap.org/data/2.5/weather?q={ville}&appid={token}'
        response=requests.get(url)
        data=response.json()
        data_extracted_from_weather_api[ville]=data

    return data_extracted_from_weather_api

def load_weather_data_to_postgre(**kwargs):
    ti=kwargs['ti']
    weather_data=ti.xcom_pull(task_ids='tsk_extract_data_from_weather_api')
    username = 'your_db_username'
    password = 'your_db_password'
    host = 'localhost'
    port = 5234
    db_name = 'your_db'
    engine= create_engine(f"postgresql://{username}:{password}@{host}:{port}/{db_name}")

    for ville, data in weather_data.items():
        coord=data.get("coord",{})
        main=data.get("main", {})
        wind=data.get("wind",{})

        df=pd.DataFrame([coord,main,wind])
        df=df.stack().T
        table_name=f'{ville}_weather'
        df.to_sql(table_name,engine,index=True,if_exists='replace')

        print(f'Data loaded to {table_name} successfully.')
        print(df.head())
        print(ville)



def load_json_weather_to_s3(**kwargs):
    S3_BUCKET_NAME = 'mspr-epsi-raw-data-1'
    ti=kwargs['ti']
    weather_data=ti.xcom_pull(task_ids='tsk_extract_data_from_weather_api')

    for ville, data in weather_data.items():
        json_data=json.dumps(weather_data)
        file_path=f'/home/lifu237/{ville}_weather_data.json'
        with open(file_path, 'w') as file:
            file.write(json_data)
        subprocess.run(['aws', 's3', 'cp', file_path, f's3://{S3_BUCKET_NAME}/json_data/'])
        print(f'Fichier json pour {ville} envoyé au s3')
    



default_args = {
    'owner': 'mspr_epsi',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 22),
    'email': 'your_email',
    'email_on_failure': False, #remplir à true avant le deploiement
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

with DAG('mspr_analytics_csv_dag', 
         default_args=default_args, 
         schedule_interval='@daily', 
         catchup=False) as dag:
    
    extract_mspr_data = PythonOperator(
        task_id='tsk_extract_mspr_data',
        python_callable=extract_data
    )

    
    extract_from_api_ = PythonOperator(
        task_id='tsk_extract_from_api',
        python_callable=extract_from_api
    )

    transform_data_ = PythonOperator(
        task_id='tsk_transform_data',
        python_callable=transform_data
    )

    load_to_postgre_ = PythonOperator(
        task_id='tsk_load_to_postgre',
        python_callable=load_to_postgre
    )

    from_pg_to_s3_daily_=PythonOperator(
        task_id='tsk_from_pg_to_s3_daily',
        python_callable= from_pg_to_s3_daily
    )


#########################  API WEATHER ###################
    extract_data_from_weather_api_=PythonOperator(
        task_id='tsk_extract_data_from_weather_api',
        python_callable=extract_data_from_weather_api
    )

    load_weather_data_to_postgre_=PythonOperator(
        task_id="tsk_load_weather_data_to_postgre",
        python_callable=load_weather_data_to_postgre
    )

    load_json_weather_to_s3_=PythonOperator(
        task_id="tsk_load_json_weather_to_s3",
        python_callable=load_json_weather_to_s3
    )


    extract_mspr_data >> extract_from_api_ >> transform_data_ >> load_to_postgre_>> from_pg_to_s3_daily_
    extract_data_from_weather_api_ >> load_weather_data_to_postgre_ >> load_json_weather_to_s3_
