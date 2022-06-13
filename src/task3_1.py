#Download iris dataset, save as csv

import requests
import os

#Get dataset
url  = 'https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data'
r = requests.get(url)

os.mkdir('tmp')
with open('tmp/iris.csv','w') as file:
	file.write(r.text.strip())
