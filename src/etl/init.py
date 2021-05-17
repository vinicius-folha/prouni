from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from proUniSchema import schema
from proUniTransformation import tranformData
import json

with open('/home/vof/Desktop/Projects/prouniProject/src/config.json', 'r') as f:
    config = json.load(f)

conf = SparkConf().setMaster("local").setAppName(config['db']['database'])
conf.set('spark.driver.extraClassPath',
         config['db']['jdbcDrivePath'])
conf.set('spark.executor.extraClassPath',
         config['db']['jdbcDrivePath'])

sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession

sqlContext.sql(f"CREATE DATABASE IF NOT EXISTS {config['db']['database']}")
sqlContext.sql(f"USE {config['db']['database']}")


def writeData(sqlContext, user, password, database):
    # In Python
    csvFiles = "/home/vof/Desktop/Projects/prouniProject/inputData/*.csv"
    # Schema as defined in the preceding example
    proUniDF = sqlContext.read.option("delimiter", ";").option(
        "header", "true").csv(csvFiles, schema=schema)
    proUniDF = tranformData(proUniDF)
    proUniDF.select('*').write \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://localhost:1433;databaseName={database}") \
        .option("dbtable", 'todasBolsas') \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .mode("overwrite") \
        .save()

writeData(sqlContext, config['db']['user'], config['db']['password'], config['db']['database'])
