#Download the parquet data file from this URL:
url = 'https://github.com/databricks/LearningSparkV2/blob/master/mlflow-project-example/data/sf-airbnb-clean.parquet'
#and load it into a Spark data frame.

#Modules required
import findspark
findspark.init()
import requests
from pyspark.sql import SparkSession

#Start spark session
spark = SparkSession \
     .builder \
     .master("local[1]") \
     .appName("Ciaran") \
     .getOrCreate()

#Get the airbnb data
r = requests.get(url)
open('', 'wb').write(r.content)