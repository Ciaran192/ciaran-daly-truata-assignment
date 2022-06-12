# Create CSV output file under out/out_2_2.txt that lists the minimum price,
# maximum price, and total row count from this data set.

from task2_1 import airbnbDF, sqlContext

#path = '.\..\out\out2_2.txt'
path = './../out/out2_2.txt'

airbnbDF.registerTempTable("airbnb_properties")

resultDF = sqlContext.sql(
    "SELECT MIN(price),MAX(price),COUNT(*) FROM airbnb_properties")

# NOTE: I tried this API to write the file but I
# was getting non descriptive bugs
# resultDF.write.option("header",True).csv(path)
# I would have included the headers by using AS in
# the SQL select statement e.g. MIN(price) AS min_price

# I just use this instead,
with open(path, 'w') as file:
    file.write('min_price, max_price, row_count\n')
    # Get the file data
    data = (resultDF.rdd.map(lambda x: ", ".join(
        [str(i) for i in x])).take(1))[0]
    file.write(data)
