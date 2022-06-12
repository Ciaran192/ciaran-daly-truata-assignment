# How many people can be accomodated by the property
# with the lowest price
#and highest rating

from task2_1 import airbnbDF,sqlContext

path = './../out/out2_4.txt'

airbnbDF.registerTempTable("airbnb_properties");

s = '''
SELECT accommodates
FROM airbnb_properties
WHERE price = (SELECT MIN(price) FROM airbnb_properties)
    AND review_scores_rating = (SELECT MAX(review_scores_rating) FROM airbnb_properties)
'''

resultDF = sqlContext.sql(s)

with open(path,'w') as file:
    #Get the file data
    data = str(((resultDF.rdd.take(1))[0]).asDict()['accommodates'])
    file.write(data)
