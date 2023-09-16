# Data_wow_DE

## Introduction
This project is create a ETL process running on airflow and ingest data into Postgres database.
This project focuses on building an ETL (Extract, Transform, Load) process using Apache Airflow for orchestrating data pipelines and ingesting data into a PostgreSQL database.

## The project comprises three primary objectives:

Docker-Compose:
* The project should include Docker Compose configurations for running orchestrating pipeline tools such as Airflow/Mage.ai/Kubeflow/MLflow

  > **For this project, I have chosen Airflow as the primary pipeline orchestrator.**.

Database Containerization:

* The database component of the project runs on PostgreSQL, which is encapsulated within a Docker container.

Efficient Data Pipeline:
* The data pipeline implemented in this project is designed for efficiency and is capable of completing its tasks within a 30-minute timeframe. This efficiency ensures optimal data processing and loading.

Database Design Diagram:
* Included in this repository is a database design diagram that visually represents the structure and relationships within the PostgreSQL schema. This diagram serves as a helpful reference for understanding the data organization.

## Before You Begin
Before begin the project please explore a following website:
* Apache Airflow - Go through [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html), which should help you understand how to setup docker airflow.

## Prerequisites
Make sure you have installed all of the following prerequisites on your development machine:
* requirements.txt - Requirements file contain essential libraries used in this project.

```bash
$ pip3 install -r requirements.txt
```

* sampledata_new.py - Python file to generate a sample parquet file use as a source file in this project.
  > After complie this python file, folder name 'data_sample' that contain parquet file will created.

```bash
$ python3 sampledata_new.py
```

## Begin

Retrieve the docker-compose.yaml file to set up and run Apache Airflow within a Docker environment.

```bash
$ curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.1/docker-compose.yaml'
```

Create a directory to be used with Apache Airflow.

```bash
$ mkdir -p ./dags ./logs ./plugins ./config
```

In the container, certain directories are mounted, allowing their contents to stay synchronized between your computer and the container.

* ./dags - Contains DAG files and utility scripts.

* ./logs - Stores logs from task execution and the scheduler.

* ./config - You can add custom log parser or add airflow_local_settings.py to configure cluster policy.

* ./plugins - You can put your custom plugins here.




