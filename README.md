# Data_wow_DE

## Introduction

There are three main assignments

Docker-Compose:
* The project should include Docker Compose configurations for running orchestrating pipeline tools such as Airflow/Mage.ai/Kubeflow/MLflow
* Database must be PostgreSQL run as a container.

Usable Data Pipeline:
* The data pipeline should be functional and capable of completing its tasks within 30 minutes. This implies efficient data processing and loading.

Database Design Diagram:
* A diagram illustrating the design of the PostgreSQL database schema. This helps others understand the structure of the data being loaded.


## Material
>1. Challenge instruction - [Shared] Data Engineer _ Data Platform Engineer Test (Hands-on).docx
>2. Source file
>>1. order_detail.csv
>>2. restaurant_detail.csv
>3. Source code
>>1. Lineman_test.ipynb (Main Script)
>>2. mapper.py, reducer.py (For build map reducer in HADOOP)

## Accessing the Source Code

You can access the source code and see a result of this project in the [GitHub Repository](https://github.com/patcharaponmai/Lineman_Senior_Data_Engineer.git).

## Requirement library
>1. Pandas
>2. Psycopg2
>3. os
>4. findspark
>5. pyspark

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
