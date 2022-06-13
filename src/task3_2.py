from pyspark.ml.classification import LogisticRegression
from pyspark.ml.linalg import Vectors
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, IndexToString,StringIndexer, VectorIndexer
from pyspark.sql.functions import col
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.sql import SparkSession


def get_dummy(df,indexCol,categoricalCols,continuousCols,labelCol):

    indexers = [ StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c))
                 for c in categoricalCols ]

    # default setting: dropLast=True
    encoders = [ OneHotEncoder(inputCol=indexer.getOutputCol(),
                 outputCol="{0}_encoded".format(indexer.getOutputCol()))
                 for indexer in indexers ]

    assembler = VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders]
                                + continuousCols, outputCol="features")

    pipeline = Pipeline(stages=indexers + encoders + [assembler])

    model=pipeline.fit(df)
    data = model.transform(df)

    data = data.withColumn('label',col(labelCol))

    return data.select(indexCol,'features','label')

def transData(data):
	return data.rdd.map(lambda r: [Vectors.dense(r[:-1]),r[-1]]).toDF(['features','label'])

#Start session
spark = SparkSession \
    .builder \
    .appName("Multinomial Logistic Regression") \
     .master("local[1]") \
    .getOrCreate()

# load data
irisDF = spark.read.csv('tmp/iris.csv')

transformed = transData(irisDF)
transformed.show(5)

labelIndexer = StringIndexer(inputCol='label',outputCol='indexedLabel').fit(transformed)
featureIndexer =VectorIndexer(inputCol="features", \
                              outputCol="indexedFeatures", \
                              maxCategories=4).fit(transformed)

logr = LogisticRegression(featuresCol='indexedFeatures', labelCol='indexedLabel',regParam=1e-5)
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
	labels=labelIndexer.labels)
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, logr,labelConverter])
model = pipeline.fit(trainingData)

