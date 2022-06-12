#Create an RDD and using Spark APIs,
# determine the top 5 purchased products
# along with how often they were purchased (frequency count).
# Write the results out in descending order of frequency

# into a file out/out_1_3.txt.

#filepath to write to
path = './../out/out1_3.txt'

#Useful lambdas for here
swapValueWithKey = lambda x:(x[1],x[0])
add = lambda x , y: x+y
tagItemsWithOne = lambda x:[(x[i],1) for i in range(0,len(x))]

#get the RDD
from task1_1 import shoppingRDD

#Do some RDD processing
indicatorRDD = shoppingRDD.flatMap(tagItemsWithOne)
productCountsRDD = indicatorRDD.reduceByKey(add)
sortedProductCountsRDD = productCountsRDD.map(swapValueWithKey).sortByKey(False).map(swapValueWithKey)
mostPopularProducts = sortedProductCountsRDD.take(5)

#Write file
with open(path,'w') as file:
    text = "\n".join([str(p) for p in mostPopularProducts])
    file.write(text)
