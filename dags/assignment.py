from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime
import requests
import psycopg2

dag = DAG(
    dag_id = 'assignment4',
    start_date = datetime(2021,2,1),
    schedule_interval = '0 2 * * *')

# Redshift connection 함수
def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "hyunsooww"
    redshift_pass = "Hyunsooww!1"
    port = 5439
    dbname = "prod"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()

def extract(link):
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    f = requests.get(link)
    return (f.text)

def transform(**kwargs):
    ti = kwargs['ti']
    text = ti.xcom_pull(key=None, task_ids = 'extract')
    lines = text.split("\n")
    return lines

def load(**kwargs):
  #멱등성 보장하려면 transaction 써야 함
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    # BEGIN;TRUNCATE TABLE;INSERT INTO TABLE VALUES ('KEEYONG', 'MALE');END;
    cur = get_Redshift_connection()
    sql = "BEGIN;"
    cur.execute(sql)
    sql = "TRUNCATE table hyunsooww.assignment4;"
    cur.execute(sql)

    ti = kwargs['ti']
    lines = ti.xcom_pull(key=None, task_ids = 'transform')

    for r in lines:
        if r != '':
            (name, gender) = r.split(",")
            print(name, "-", gender)
            if name == 'name' and gender == 'gender':
              continue
            sql = "INSERT INTO hyunsooww.assignment4 VALUES ('{name}', '{gender}')".format(name=name, gender=gender)
            print(sql)
            cur.execute(sql)
        else:
            print('line read error')
    sql = "SELECT count(1) FROM hyunsooww.assignment4"
    cur.execute(sql)
    sql = "END;"
    cur.execute(sql)


extract = PythonOperator(task_id = 'extract', 
                        python_callable = extract, provide_context = True, dag = dag)

transform = PythonOperator(task_id = 'transform', 
                            python_callable = transform, provide_context = True, dag = dag)

load = PythonOperator(task_id = 'load', 
                        python_callable = load, provide_context = True, dag = dag)


extract >> transform >> load