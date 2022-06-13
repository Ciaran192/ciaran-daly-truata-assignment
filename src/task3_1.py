#curl -L "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data" -o /tmp/iris.csv

from pyspark.sql import SparkSession

#Download iris dataset

import requests
import os

mapCategories = {
	'Iris-setosa':'0',
	'Iris-versicolor':'1',
	'Iris-virginica':'2'
}

#Get dataset
url  = 'https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data'
r = requests.get(url)

#Start spark session
spark = SparkSession \
     .builder \
     .master("local[1]") \
     .appName("Ciaran") \
     .getOrCreate()

#r.text.strip()
#0 1:0.111111 2:-0.583333 3:0.355932 4:0.5 
rows = list(map(lambda x:x.split(',') , r.text.strip().split('\n')))
irisRDD = spark.sparkContext.parallelize(list(map(lambda x:x.split(',') , r.text.strip().split('\n'))))
svmlibIrisRDD = irisRDD.map(lambda x:mapCategories[x[4]] + ' 1:' + x[0] + ' 2:' + x[1] + ' 3:' + x[2] + ' 4:' + x[3] + ' ')
svmlibIrisRDD.saveAsTextFile('iris.txt')
