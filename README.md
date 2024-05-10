# Data Science and Data Engineering Final Project

This project contains scraping data from Scopus, automatic processing using Apache Airflow, storing data using Redis, modeling "Abstract to Title Generator" using T5 model, creating GUI for using model, and visualization with Gephi and Power BI.

This is a part of project in Data Science and Data Engineering 2/2023.

## Main Features

- [DE] Scraping data from Scopus REST API
- [DE] Automating process for 2 processes using Apache Airflow
    - Scraping data and push data in Redis (processing daily)
    - Retrieve data from Redis and export or save as CSV file
- [DE] Storing data in Redis
- [ML] Modelling "Abstract to Title Generator" using T5 model with pretrained model as "T5-small"
- [ML] Creating GUI for user to generate title from abstract easily using streamlit
- [VIS] Graph visualization using Gephi
- [VIS] Visualization using Power BI (Map, Graph, Cloud)
- [VIS] Additional visualization in Google Colab
