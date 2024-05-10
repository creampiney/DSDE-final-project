from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import datetime
import redis
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

def pull_from_redis(data):
    rd = redis.Redis(host='ec2-44-210-142-191.compute-1.amazonaws.com', port=6379, password='redis', charset="utf-8", decode_responses=True)
    result = rd.hgetall(data)
    return result

def multithread_pull_redis(items):
    results = []
    with ThreadPoolExecutor(max_workers=256) as executor:
        futures = [executor.submit(pull_from_redis, item) for item in items]
        for future in as_completed(futures):
            results.append(future.result())
    return results

def check_redis_keys():
    rd = redis.Redis(host='ec2-44-210-142-191.compute-1.amazonaws.com', port=6379, password='redis', charset="utf-8", decode_responses=True)
    print("found:",len(rd.keys()))
    print(rd.keys())
    print("found:",len(rd.keys('research:*')))
    print(rd.keys('research:*'))
    print("found:",len(rd.keys('author:*')))
    print(rd.keys('author:*'))
    print("found:",len(rd.keys('affiliation:*')))
    print(rd.keys('affiliation:*'))

def research2csv():
    research_results = []
    rd = redis.Redis(host='ec2-44-210-142-191.compute-1.amazonaws.com', port=6379, password='redis', charset="utf-8", decode_responses=True)
    research = multithread_pull_redis(rd.keys('research:*')[0:1000])
    research_results.append(research)
    df = pd.DataFrame(research_results)
    df.to_csv(f'research_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}.csv', index=False)
    
def author2csv():
    rd = redis.Redis(host='ec2-44-210-142-191.compute-1.amazonaws.com', port=6379, password='redis', charset="utf-8", decode_responses=True)
    author_results = multithread_pull_redis(rd.keys('author:*')[0:1000])
    df = pd.DataFrame(author_results)
    df.to_csv(f'author_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}.csv', index=False)

def affiliation2csv():
    rd = redis.Redis(host='ec2-44-210-142-191.compute-1.amazonaws.com', port=6379, password='redis', charset="utf-8", decode_responses=True)
    affiliation_results = multithread_pull_redis(rd.keys('affiliation:*')[0:1000])
    df = pd.DataFrame(affiliation_results)
    df.to_csv(f'affiliation_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}.csv', index=False)
    
def finish():
    print('finish')
    
dag = DAG(
    'pull_data_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval=None,
    catchup=False
)

check_redis_keys_task = PythonOperator(
    task_id='check_redis_keys',
    python_callable=check_redis_keys,
    dag=dag
)

research2csv_task = PythonOperator(
    task_id='research2csv',
    python_callable=research2csv,
    dag=dag
)

author2csv_task = PythonOperator(
    task_id='author2csv',
    python_callable=author2csv,
    dag=dag
)

affiliation2csv_task = PythonOperator(
    task_id='affiliation2csv',
    python_callable=affiliation2csv,
    dag=dag
)

finish_task = PythonOperator(
    task_id='finish',
    python_callable=finish,
    dag=dag
)

check_redis_keys_task >> [research2csv_task, author2csv_task, affiliation2csv_task] >> finish_task
