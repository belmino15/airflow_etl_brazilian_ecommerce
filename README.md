# Pipeline do Airflow para análise de dados

## Resumo:
- Requisitos;
- Repositório;
- Introdução;
- Instruções;
- Resultados;
- Por vir;
- Conjunto de dados.


## Requisitos:
- Python;
- Docker;
- Qualquer cliente SQL;

## Repository:
- airflow - Localização de registros e dags de fluxo de ar.
- code - Localização de qualquer código autônomo;
- files - Localização onde os conjuntos de dados precisam estar;
- jupyters - Localização de notebooks usados.

## Introduction:

![kaggle_db](https://i.ytimg.com/vi/Uz26FqGE9tE/maxresdefault.jpg)


O aumento do volume de dados armazenados pelas empresas tem dado origem a novos empregos nas empresas. Profissionais como Analista de Dados, Engenheiro de Dados e Cientista de Dados têm sido cada vez mais solicitados a trabalhar os dados que a empresa possui e agregar valor à empresa.

Neste projeto, veremos o trabalho normalmente realizado por um Engenheiro de Dados para facilitar o acesso aos dados por Analistas de Dados.

Este projeto tem como objetivo apresentar ferramentas e práticas que permitem ao Engenheiro de Dados disponibilizar automaticamente os dados para o Analista de Dados.

A intenção é permitir ao Analista de Dados menos esforço e maior velocidade na criação de visualizações e na geração de insights com dados confiáveis ​​que reflitam o estado atual do negócio.

## Instruções:

Inicie o Mysql Docker Container:
    
    docker run -d --name mysql_server -p "3306:3306" -e MYSQL_ROOT_PASSWORD=sqlpass mysql

Inicie o serviço Airflow:

    docker run -d -p 8080:8080 -v "$PWD/files:/files" -v "$PWD/airflow/dags:/opt/airflow/dags/" -v "$PWD/airflow/logs:/opt/airflow/logs/" --entrypoint=/bin/bash --name airflow_service apache/airflow:2.1.1-python3.8 -c '(airflow db init && airflow users create --username admin --password airflowpass --firstname first --lastname last --role Admin --email admin@example.org); airflow webserver & airflow scheduler'

Criar banco de dados:

    python code/create_databases.py

No Client SQL você verá os bancos de dados criados:

![empty_db](https://github.com/belmino15/airflow_etl_brazilian_ecommerce/blob/master/images/empty_db.png)

No Airflow Webserver, você vê:

![airflow_1](https://github.com/belmino15/airflow_etl_brazilian_ecommerce/blob/master/images/airflow_1.png)

Habilite e inicie * "file_stage_etl" * e espere concluir.

![airflow_2](https://github.com/belmino15/airflow_etl_brazilian_ecommerce/blob/master/images/airflow_2.png)

Então, habilite e inicie * "stage_dw_etl" * e espere concluir.

![airflow_3](https://github.com/belmino15/airflow_etl_brazilian_ecommerce/blob/master/images/airflow_3.png)

O banco de dados será assim:

![complete_db](https://github.com/belmino15/airflow_etl_brazilian_ecommerce/blob/master/images/complete_db.png)

## Resultados:

O [link] (https://www.kaggle.com/olistbr/brazilian-ecommerce?select=olist_sellers_dataset.csv) mostra que os dados originais possuem a estrutura mostrada a seguir.

![kaggle_db](https://i.imgur.com/HRhd2Y0.png)

Após a reformulação, os dados ficarão organizados da seguinte forma.

![dw_compras](https://github.com/belmino15/airflow_etl_brazilian_ecommerce/blob/master/images/dw_compras.png)

## Por vir:
- Atualização diária;
- Novas visualizações:
    - Métodos de Pagamento;
    - Avaliações públicas;
    - Rastreamento de embarque.

## Conjunto de dados:
Kaggle: Avaiable in this [link](https://www.kaggle.com/olistbr/brazilian-ecommerce?select=olist_sellers_dataset.csv).