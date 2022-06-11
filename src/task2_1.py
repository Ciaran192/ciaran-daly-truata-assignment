#Download the parquet data file from this URL:
url = 'https://github.com/databricks/LearningSparkV2/blob/master/mlflow-project-example/data/sf-airbnb-clean.parquet/'
#and load it into a Spark data frame.

#Extra info for URL
extraURLInfo = 'part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet?raw=true'

#Modules required
import findspark
findspark.init()
from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkContext
import requests

#Start spark session
spark = SparkSession \
     .builder \
     .master("local[1]") \
     .appName("Ciaran") \
     .getOrCreate()
     
#Create a SQL context
sc = SparkContext.getOrCreate()
sqlContext = SQLContext.getOrCreate(sc)     

#Get the airbnb data
r = requests.get(url + extraURLInfo)
tempFile = 'airbnb.parquet'

with open(tempFile, 'wb') as file:
    file.write(r.content)
    
#Load it to a spark dataframe
airbnbDF = sqlContext.read.parquet(tempFile)
