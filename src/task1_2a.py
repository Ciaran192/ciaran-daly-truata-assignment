#Using Spark's RDD API,
#create a list of all (unique) products present in the transactions.
#Write out this list to a text file

from task1_1 import shoppingRDD

path = './../out/out1_2a.txt'

#Nice one liner,
#1 Flatten the structure of the RDD,
#2 Gets the distinct elements,
#3 Return it as a list.
uniqueItems = shoppingRDD.flatMap(lambda x:x).distinct().collect()

#Write the list to disk
with open(path,'w') as file:
    text = "\n".join(uniqueItems).strip()
    file.write(text)
