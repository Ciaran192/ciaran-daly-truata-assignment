#Download the data file from the above location 
#and make it accessible to Spark

#Get pyspark
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

#Get the shopping lists
url  = 'https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv'
r = requests.get(url)
shoppingLists =list(map(lambda x:x.split(',') , r.text.strip().split('\n')))

#Load into RDD
shoppingRDD = spark.sparkContext.parallelize(shoppingLists)
