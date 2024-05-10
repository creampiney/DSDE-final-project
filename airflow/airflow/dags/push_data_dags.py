from __future__ import annotations

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import datetime
import redis
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
import numpy as np
from datetime import date, datetime
import redis
import pandas as pd
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Generator


def upload_to_redis():
    # data = output values (tuple) from scraping_scopus()
    data = res

    rd = redis.Redis(host='ec2-44-210-142-191.compute-1.amazonaws.com', charset="utf-8", decode_responses=True, password='redis')

    researches_list = data[0]
    authors_list = data[1]
    affiliations_list = data[2]

    def push_to_redis(key, data):
        rd.hset(f"{key}:{data['id']}", mapping=data)
    
    def multithread_push_redis(key, items):
        c = 0
        with ThreadPoolExecutor(max_workers=64) as executor:
            futures = [executor.submit(push_to_redis, key, item) for item in items]
            for future in as_completed(futures):
                c += 1

    multithread_push_redis("research", researches_list)
    multithread_push_redis("author", authors_list)
    multithread_push_redis("affiliation", affiliations_list)
    
def scraping_scopus():
    queryText = "data"
    counts = 100
    # Your API key obtained from Elsevier Developer Portal
    API_KEY = '???'

    researches_id_set = set()
    authors_id_set = set()
    affiliations_id_set = set()

    # Scopus API endpoint for document search
    SCOPUS_API_URL = 'https://api.elsevier.com/content/search/scopus'
    
    def query_scopus(query, count, year, start):
        params = {
            'apiKey': API_KEY,  # Include your API key here
            'query': query,
            'count': count,
            'date': year,
            'start': start,
        }
    
        response = requests.get(SCOPUS_API_URL, params=params)
    
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            return None

    def scopus_finder(queryText, counts):
        today_date = date.today()
        today = datetime.strptime(f"{today_date}", "%Y-%m-%d")
        first_date_of_year = datetime.strptime(f"{today_date.year}-01-01", "%Y-%m-%d")
        start = (today - first_date_of_year).days
        
        results = query_scopus(queryText, counts, today_date.year, start)
        
        if results:
            # print("Search Successful")
            return [e["dc:identifier"].split(":")[1] for e in results["search-results"]["entry"]]
        else:
            # print("Search Failed")
            return []

    def get_full_text_id(id, api_key):
        crossref_api_url = f'https://api.elsevier.com/content/abstract/scopus_id/{id}'
        response = requests.get(crossref_api_url ,headers={"Accept" : "application/json","X-ELS-APIKey" : api_key },params={"view" : "FULL"})
        if response.status_code == 200:
            data = json.loads(response.text)
            return data
        else:
            print(f"Failed to fetch full-text. Status code: {response.status_code}")
        return None

    fetched_id_list = scopus_finder(queryText, counts)

    authors_list = []
    affiliations_list = []
    papers_list = []

    def extract_json_from_data(data):
        # Check if there is key in data
        assert "abstracts-retrieval-response" in data
    
        data = data["abstracts-retrieval-response"]
    
        if data.get("item").get("bibrecord").get("tail") is None:
            new_ref_id_list = []
        else:
            if type(data.get("item").get("bibrecord").get("tail").get("bibliography").get("reference")) is dict:
                ref_id_list = [data.get("item").get("bibrecord").get("tail").get("bibliography").get("reference").get("ref-info").get("refd-itemidlist").get("itemid")]
            else:
                ref_id_list = [field.get("ref-info").get("refd-itemidlist").get("itemid") for field in data.get("item").get("bibrecord").get("tail").get("bibliography").get("reference")] if data.get("item").get("bibrecord").get("tail") else []
            new_ref_id_list = []
            for r in ref_id_list:
                if type(r) is list:
                    new_ref = [g.get("$") for g in r if g.get("@idtype") in "SGR"][0]
                else:
                    new_ref = r.get("$")
                new_ref_id_list.append(new_ref)
    
        # Authors
        if data.get("authors").get("author"):
            for author in data.get("authors").get("author"):
                author_id = author.get("@auid")
                if author_id in authors_id_set:
                    continue
    
                if type(author.get("affiliation")) is dict:
                    aff_list = [author.get("affiliation")]
                else :
                    aff_list = author.get("affiliation") if author.get("affiliation") else []
    
                authors_id_set.add(author_id)
                authors_list.append({
                    "id": author_id,
                    "given_name": author.get("ce:given-name"),
                    "initials": author.get("ce:initials"),
                    "surname": author.get("ce:surname"),
                    "indexed_name": author.get("ce:indexed-name"),
                    "affiliations_id": "|".join(set([a.get("@id") for a in aff_list]))
                })
    
        # Affiliations
        if data.get("affiliation"):
            if type(data.get("affiliation")) is dict:
                aff_list = [data.get("affiliation")]
            else:
                aff_list = data.get("affiliation") if data.get("affiliation") else []
                
            for aff in aff_list:
                
                if aff.get("@id") in affiliations_id_set:
                    continue
                
    
                affiliations_id_set.add(aff.get("@id"))
                affiliations_list.append({
                    "id": aff.get("@id"),
                    "name": aff.get("affilname"),
                    "city": aff.get("affiliation-city"),
                    "country": aff.get("affiliation-country"),
                })
    
        # Research
        return {
            "id": data.get("coredata").get("dc:identifier").split(":")[1],
            "doi": data.get("coredata").get("prism:doi"),
            "eid": data.get("coredata").get("eid"),
            "cover_date": data.get("coredata").get("prism:coverDate"),
            "title": data.get("item").get("bibrecord").get("head").get("citation-title"),
            "abstract": data.get("item").get("bibrecord").get("head").get("abstracts"),
            "subject_areas": "|".join(set([field.get("@abbrev") for field in data.get("subject-areas").get("subject-area")])) if data.get("subject-areas").get("subject-area") else "",
            "auth_keywords": "|".join(set([field.get("$") for field in data.get("auth-keywords")])) if data.get("auth-keywords") else "",
            "authors_id": "|".join(set([field.get("@auid") for field in data.get("authors").get("author")])) if data.get("authors").get("author") else "",
            "citedby_count": data.get("coredata").get("citedby-count"),
            "ref_count": data.get("item").get("bibrecord").get("tail").get("bibliography").get("@refcount") if data.get("item").get("bibrecord").get("tail") else 0,
            "ref_ids": "|".join(new_ref_id_list),
            "published_year": data.get("coredata").get("prism:coverDate").split("-")[0],
            "published_month": data.get("coredata").get("prism:coverDate").split("-")[1],
            "published_day": data.get("coredata").get("prism:coverDate").split("-")[2],
        }

    errcnt = 0
    
    for research_id in fetched_id_list:
        if research_id in researches_id_set:
            continue
            
        try:
            data = get_full_text_id(research_id, API_KEY)
            json_obj = extract_json_from_data(data)
            papers_list.append(json_obj)
            researches_id_set.add(research_id)
        except Exception as err:
            errcnt += 1

    return (papers_list, authors_list, affiliations_list)

dag = DAG(
    'push_data_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 0 * * *',
    catchup=False
)

scraping_scopus_task = PythonOperator(
    task_id='scraping_data',
    python_callable=scraping_scopus,
    dag=dag
)

upload_to_redis_task = PythonOperator(
    task_id='upload_to_redis',
    python_callable=upload_to_redis,
    dag=dag
)

scraping_scopus_task >> upload_to_redis_task