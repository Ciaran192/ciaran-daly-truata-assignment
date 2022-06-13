#Again, using Spark only,
#write out the total count of products to a text file

#Get the RDD
from task1_1 import shoppingRDD


#Path to writ the file to
path = './../out/out1_2b.txt'

#Flatten the structure, get the distinct items, count.
#I am assuming that the amount of unique items is
#what is meant by "count of products"
c = shoppingRDD.flatMap(lambda x:x).distinct().count()

#Write file
with open(path,'w') as file:
    text = 'Count:\n' + str(c)
    file.write(text)
