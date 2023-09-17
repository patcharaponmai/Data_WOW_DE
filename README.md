# Data_WOW_Data_Engineer

## Introduction
This project focuses on building an ETL (Extract, Transform, Load) process using Apache Airflow for orchestrating data pipelines and ingesting data into a PostgreSQL database.

## The project comprises four primary objectives:

Docker-Compose:
* The project should include Docker Compose configurations for running orchestrating pipeline tools such as Airflow/Mage.ai/Kubeflow/MLflow

  > **For this project, I have chosen Airflow as the primary pipeline orchestrator.**

Database Containerization:

* The database component of the project runs on PostgreSQL, which is encapsulated within a Docker container.

Efficient Data Pipeline:
* The data pipeline implemented in this project is designed for efficiency and is capable of completing its tasks within a 30-minute timeframe. This efficiency ensures optimal data processing and loading.

Database Design Diagram:
* Included in this repository is a database design diagram that visually represents the structure and relationships within the PostgreSQL schema. This diagram serves as a helpful reference for understanding the data organization.

## Before begin the project
Please explore a following website:
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

## Modify file

Modify the `docker-compose.yaml` file by including below command in the volume variable. 

```
${AIRFLOW_PROJ_DIR:-.}/data_sample:/opt/airflow/data_sample 
```
This change is necessary to set up the source file path with the 'data_sample' folder created during the Prerequisites step. The result after modifed is shown in figure below.

<img width="617" alt="Screen Shot 2566-09-16 at 14 57 44" src="https://github.com/patcharaponmai/Data_wow_DE/assets/140698887/2d3a812b-9908-45c7-919a-c8e310f4ab8e">


Save some variable value in file `./.env`

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Create file `./config/airflow.cfg` and add following command in order to avoid heartbeat failed due to long task execution

```bash
[scheduler]
scheduler_heartbeat_sec = 1800  # Increase this value to allow longer task execution
```

## Initialize database

Regardless of your operating system, you must execute database migrations and establish the initial user account. To achieve this, execute the following command:

```bash
$ docker-compose up airflow-init
```
When the execute command done you will get as following figure.

<img width="915" alt="Screen Shot 2566-09-16 at 15 20 38" src="https://github.com/patcharaponmai/Data_wow_DE/assets/140698887/1013f4e2-181b-4799-820a-66feecf6d742">


After completing this process, you will have a default account with the username `airflow` and password `airflow` to log in via the Apache Airflow UI.


## Start service

Execute `docker-compose up` to initiate all the services within our Docker environment.

```bash
$ docker-compose up
```

## Access Apache Airflow UI

The webserver is available at:

```
http://localhost:8080
```

<img width="1302" alt="Screen Shot 2566-09-16 at 15 30 09" src="https://github.com/patcharaponmai/Data_wow_DE/assets/140698887/a39a6f29-996b-4b7d-ae2d-174a7cfbe632">


## Before trigger DAG process

We need to establish a connection with a PostgreSQL database, follow these steps:

After login, click `Admin` > `Connections` 

<img width="1427" alt="Screen Shot 2566-09-16 at 15 33 57" src="https://github.com/patcharaponmai/Data_wow_DE/assets/140698887/91b83646-17a2-4c9a-9113-3b966b59fc59">

Then select ➕ button to create connection.

<img width="613" alt="Screen Shot 2566-09-16 at 15 40 41" src="https://github.com/patcharaponmai/Data_wow_DE/assets/140698887/f22a9d14-da56-41b4-9e50-b635607e358c">


* Connection Id - `postgres_default`

* Connection Type - Search `Postgres` in drop down

* Host - `postgres`

* Schema - `airflow`

* Login - `airflow`

* Host - `5432`

Click `save` to save the configuration.

## Trigger DAG process

Return to the Apache Airflow UI homepage. In the `DAG` column, locate the DAG named `ETL_process`, as specified in the main script.  


<img width="1440" alt="Screen Shot 2566-09-16 at 15 50 58" src="https://github.com/patcharaponmai/Data_wow_DE/assets/140698887/39cbbdba-6915-40da-9f25-7ed1fad7129f">  


After access to our DAG:ETL_process, you will see the flow in project my select `Graph` under `DAG:ETL_process`


<img width="1440" alt="Screen Shot 2566-09-16 at 15 57 36" src="https://github.com/patcharaponmai/Data_wow_DE/assets/140698887/921553e0-5fb0-4340-91e5-2e62e33136e9">

Click the ▶️ (play) button in the top-right corner to trigger the DAG. When the left chart or graph processors are completely green, it means that our DAG process is complete!

# 🎉 🎊  End project 🎉 🎊 
==================================================
