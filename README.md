# Airflow Pipeline for Data Analysis

## Summary:
- Requirements;
- Repository;
- Introduction;
- Instructions;
- Results;
- Upcoming;
- Dataset.


## Requirements:
- Python;
- Docker;
- Any SQL Client;

## Repository:
- airflow - Location of airflow dags and logs.
- code - Location of any standalone code;
- files - Location where the datasets need to be;
- jupyters - Location of used notebooks

## Introduction:

![kaggle_db](https://i.ytimg.com/vi/Uz26FqGE9tE/maxresdefault.jpg)


The increase in the volume of data stored by companies has given rise to new jobs in companies. Professionals such as Data Analyst, Data Engineer and Data Scientist, have been increasingly required to work the data that the company has in its possession and bring value to the company.

In this project, we will look at the work normally performed by a Data Engineer in order to facilitate data access by Data Analysts.

This project aims to present tools and practices that allow the Data Engineer to automatically make data available to the Data Analyst.

The intention is to allow the Data Analyst less effort and greater speed in creating views and generating insights with reliable data that reflect the current state of the business.

## Instructions:

Start Mysql Docker Container:
    
    docker run -d --name mysql_server -p "3306:3306" -e MYSQL_ROOT_PASSWORD=sqlpass mysql

Start Airflow Service:

    docker run -d -p 8080:8080 -v "$PWD/files:/files" -v "$PWD/airflow/dags:/opt/airflow/dags/" -v "$PWD/airflow/logs:/opt/airflow/logs/" --entrypoint=/bin/bash --name airflow_service apache/airflow:2.1.1-python3.8 -c '(airflow db init && airflow users create --username admin --password airflowpass --firstname first --lastname last --role Admin --email admin@example.org); airflow webserver & airflow scheduler'

Create Database:

    python code/create_databases.py

In the SQL Client you will see the created databases:

![empty_db](images\empty_db.png)

In the Airflow Webserver you see:

![airflow_1](images\airflow_1.png)

Enable and trig *"file_stage_etl"* and wait to be done.

![airflow_2](images\airflow_2.png)

Then, enable and trig *"stage_dw_etl"* and wait to be done.

![airflow_3](images\airflow_3.png)

The database will then be like this:

![complete_db](images\complete_db.png)

## Results:

The [link](https://www.kaggle.com/olistbr/brazilian-ecommerce?select=olist_sellers_dataset.csv) shows that the original data has the structure shown below.

![kaggle_db](https://i.imgur.com/HRhd2Y0.png)

After the reformulation, the data will be organized as follows.

![dw_compras](images\dw_compras.png)

## Upcoming:
- Daily Update;
- New views:
    - Payment methods;
    - Public reviews;
    - Shipment tracking.

## Dataset:
Kaggle: Avaiable in this [link](https://www.kaggle.com/olistbr/brazilian-ecommerce?select=olist_sellers_dataset.csv).