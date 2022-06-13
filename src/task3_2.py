from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression

from pyspark.sql import SparkSession

#Start spark session
spark = SparkSession \
     .builder \
     .master("local[1]") \
     .appName("Ciaran") \
     .getOrCreate()

labelMap = {
    'Iris-setosa':0,
    'Iris-versicolor':1,
    'Iris-virginica':2
}
invLabelMap = {v:k for k,v in labelMap.items()}

irisRDD = spark.sparkContext.textFile('tmp/iris.csv')
irisRDD = irisRDD.map(lambda x:x.split(','))
irisRDD = irisRDD.map(lambda x:(labelMap[x[4]],Vectors.dense(*[float(x[0]),float(x[1]),float(x[2]),float(x[3])])))
irisDF = irisRDD.toDF(['label','features'])

lr = LogisticRegression(maxIter=100, regParam=1e-5)

irisModel = lr.fit(irisDF)

test = spark.createDataFrame([
    (labelMap['Iris-setosa'], Vectors.dense([5.1, 3.5, 1.4, 0.2])),
    (labelMap['Iris-virginica'], Vectors.dense([6.2, 3.4, 5.4, 2.3])),], ["label", "features"])
#pred_data = spark.createDataFrame(
#    [(5.1, 3.5, 1.4, 0.2),
#    (6.2, 3.4, 5.4, 2.3)],
#    ["sepal_length", "sepal_width", "petal_length", "petal_width"])

#prediction = irisModel.transform(pred_data.rdd.map(lambda x:Vectors.dense(*[x[0],x[1],x[2],x[3]])).toDF(["features"]))
#predictionDF = pred_data.rdd.map(lambda x:Vectors.dense(*[x[0],x[1],x[2],x[3]])).toDF(["features"])
#predictionDF.show()
#prediction = irisModel.transform(pred_data)
prediction = irisModel.transform(test)

result = prediction.select("features", "label","prediction")
predicitedFlowers = [invLabelMap[int(row.prediction)] for row in result.select('prediction').collect()]

with open('../out/out3_2.txt','w') as file:
    file.write('class\n')
    file.write('\n'.join(predicitedFlowers))
