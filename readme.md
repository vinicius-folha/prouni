# ProUni Dashboard
## Dashboard with ETL using PROUNI data
<p align="center">This project use a open datasource to develop a dashboard and ETL with Pypark and Dash</p>

## Table of Contents
=================
<!--ts-->
   * [Table of Contents](#Table-of-Contents)
   * [About](#About)
   * [Prerequisites](#Prerequisites)
   * [To Run](#To-Run)
   * [Features](#Features)
   * [Improves](#Improves)
   * [🛠 Technology](#🛠-Technology)
   * [📷 Pictures](#📷-Pictures)
<!--te-->
## About
Dashboard and ETL project built with data from ProUni (2016-2018) using Dash, PySpark, SQL Server.
<h4 align="center"> 
	🚧  ProUni Dashbord  🚀 Under maintenance...  🚧
</h4>

### Prerequisites


Before you begin, you will need to have the following tools installed on your machine:
[Java JDK](https://www.oracle.com/br/java/technologies/javase-jdk11-downloads.html), [Scala](https://www.scala-lang.org/download/) and [Git](https://git-scm.com/). 

### 🎲 Setting up

```bash
# Install JAVA JDK, Scala, Git 
$ sudo apt install default-jdk scala git -y 

# Check installation
$ java -version; javac -version; scala -version; git --version 

# Download Spark
$ curl -O https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz

# Extract file
$ tar xvf spark-3.1.1-bin-hadoop3.2.tgz

# Move folder to /opt/ repository:
$ sudo mv spark-3.1.1-bin-hadoop3.2/ /opt/spark 

# Set Spark environment
# Setting environment varible:
# Open file
$ gedit ~/.bashrc
# Add lines:
    export SPARK_HOME=/opt/spark
    export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
# Activate the changes:
$ source ~/.bashrc

# Recommend create a Virtual Env before this command
pip3 install -r requirements.txt
```
## To Run
```bash
python3 src/etl/init.py
python3 src/main.py
```
### Features

- [x] ETL
- [x] Filter by Year
- [x] Generic grouping chart
- [x] Line chart

### Improves


- [] Add Database URL on config
- [] Use Environment Variables
- [] Refatoring Dashboard and ETL
- [] Use relative path for files
- [] Validate data entry on ETL
- [] More graph
   - [] boxplot
   - [] Map
- [] Improve the layout
- [] Separate SQL Tables
- [] Docker

### 🛠 Technology

The following tools were used in the construction of the project:

- [Pyspark](https://spark.apache.org/docs/latest/api/python/)
- [Dash](https://dash.plotly.com/)

### 📷 Pictures
![Alt text](screenshots/Example_Idade_RACA.png?raw=true "Example: Idade x Raça")

![Alt text](screenshots/Example_Idade_Turno.png?raw=true "Example: Idade x Turno")

![Alt text](screenshots/Example_Modalidade_Raca.png?raw=true "Example: Modalidade x Raca")
