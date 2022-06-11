#Calculate the average number of bathrooms and bedrooms 
#across all the properties listed in this data set 
#with a price of > 5000
#and a review score being exactly equalt to 10

#I assume that what is being asked for here is 
#To filter on 

from task2_1 import airbnbDF,sqlContext

path = '.\..\out\out2_3.txt'

airbnbDF.registerTempTable("airbnb_properties");

#Select statment
s = '''
SELECT AVG(bathrooms),AVG(bedrooms) 
FROM airbnb_properties 
WHERE price > 5000.0 
AND (review_scores_rating = 10.0 OR 
     review_scores_accuracy = 10.0 OR 
     review_scores_cleanliness = 10.0 OR 
     review_scores_checkin = 10.0 OR 
     review_scores_communication = 10.0 OR 
     review_scores_location = 10.0 OR 
     review_scores_value = 10.0)
'''

#Run Query
resultDF = sqlContext.sql(s)

#Write result
with open(path,'w') as file:
    file.write('avg_bathrooms,avg_bedrooms\n')
    #Get the file data
    data = (resultDF.rdd.map(lambda x:",".join([str(i) for i in x])).take(1))[0]
    file.write(data)

'''
host_is_superhost
cancellation_policy
instant_bookable
host_total_listings_count
neighbourhood_cleansed
latitude
longitude
property_type
room_type
accommodates
bathrooms
bedrooms
beds
bed_type
minimum_nights
number_of_reviews
review_scores_rating
review_scores_accuracy
review_scores_cleanliness
review_scores_checkin
review_scores_communication
review_scores_location
review_scores_value
price
bedrooms_na
bathrooms_na
beds_na
review_scores_rating_na
review_scores_accuracy_na
review_scores_cleanliness_na
review_scores_checkin_na
review_scores_communication_na
review_scores_location_na
review_scores_value_na
'''