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

```bash
$ python3 sampledata_new.py
```

## Installation

>1. PostgreSQL Databsae
```
# install PostgreSQL
!apt install postgresql postgresql-contrib &>log

# start PostgreSQL serviecs
!service postgresql start

# Create root user
!sudo -u postgres psql -c "CREATE USER root WITH SUPERUSER"

# Create database
!sudo -u postgres createdb challenge
```

>2. Pre-Requisites for HADOOP installation
```
# install java
!apt-get install openjdk-8-jdk-headless -qq > /dev/null

#create java home variable 
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
```
>3. HADOOP Installation
```
#download HADOOP (NEW DOWNLOAD LINK)
!wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz

#extract the file
!tar -xzvf hadoop-3.3.0.tar.gz

#copy the hadoop file to user/local
!cp -r hadoop-3.3.0/ /usr/local/

#find  the default Java path
!readlink -f /usr/bin/java | sed "s:bin/java::"

#run Hadoop
!/usr/local/hadoop-3.3.0/bin/hadoop

#create input folder for demonstration exercise
!mkdir ~/testin

#copy sample files to the input folder
!cp /usr/local/hadoop-3.3.0/etc/hadoop/*.xml ~/testin

#check that files have been successfully copied to the input folder
!ls ~/testin

#run the mapreduce example program
!/usr/local/hadoop-3.3.0/bin/hadoop jar /usr/local/hadoop-3.3.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0.jar grep ~/testin ~/testout 'allowed[.]*'

#download and extract 20 newsgroup dataset
!wget http://qwone.com/~jason/20Newsgroups/20news-18828.tar.gz
!tar -xzvf 20news-18828.tar.gz

# This step require both python file put on working directory path
!chmod u+rwx /content/mapper.py
!chmod u+rwx /content/reducer.py

!/usr/local/hadoop-3.3.0/bin/hadoop jar /usr/local/hadoop-3.3.0/share/hadoop/tools/lib/hadoop-streaming-3.3.0.jar -input /content/20news-18828/alt.atheism/49960 -output ~/tryout -file /content/mapper.py  -file /content/reducer.py  -mapper 'python mapper.py'  -reducer 'python reducer.py'
```

>4. Pyspark installation
```
#download SPARK (NEW DOWNLOAD LINK)
!wget -q http://apache.osuosl.org/spark/spark-3.3.3/spark-3.3.3-bin-hadoop3.tgz

#extract the spark file to the current folder
!tar xf spark-3.3.3-bin-hadoop3.tgz

#create spark home variable 
os.environ["SPARK_HOME"] = "/content/spark-3.3.3-bin-hadoop3"

#install findspark
#findspark searches pyspark installation on the server 
#and adds pyspark installation path to sys.path at runtime 
#so that pyspark modules can be imported

!pip install -q findspark

#import findspark
import findspark
findspark.init()

!pip install pyspark
```
