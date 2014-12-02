import matplotlib.pyplot as plt
import numpy as np
import math as ma
import numpy as np
import urllib2
import json
import time
import pandas as pd
from sklearn import cluster, datasets

ext = '/Users/patrickkuiper/Desktop/Academic/AC209/cs209-project/movieRTAndBudgetData.csv' ##this is where you store the movies on your hard drive
data = pd.read_csv(ext, sep = ',', index_col=6)
movie_name=data.Name
names=[]
for i in data.index:#builds the proper movie keys with plus signs for urls
    names.append(movie_name[i].replace(' ', '+'))
my_key = 'fwqk8nsy6fuuapxgkbq4c72u'
urls=[] #urls for each movie
for i in xrange(0,len(movie_name)):##builds the list of urls for each movie
    urls.append('http://api.rottentomatoes.com/api/public/v1.0/movies.json?q='+
                names[i]+'&page_limit=10&page=1&apikey=' + my_key)
                
source = urllib2.urlopen(urls[12]).read() #changes which movie displayed 
info = json.loads(source.decode('utf-8'))
revURL='?review_type=all&page_limit=20&page=1&country=us&apikey='
keys1=info.keys() #keys for highest level dictionary
keys2=info[keys1[0]][0].keys() #keys for secondary level dictionary
keys3=info[keys1[0]][0][keys2[1]].keys() #important link keys
reviewURL=str(info[keys1[0]][0][keys2[1]][keys3[0]])+revURL+my_key
source2 = urllib2.urlopen(reviewURL).read()
review = json.loads(source2.decode('utf-8'))##pulls the review
reviewKeys=review.keys() #keys for moview review dict

df2 = pd.DataFrame(columns=["Reviews"], index=movie_name)


for j in xrange(0,1019):
    time.sleep(1)
    source = urllib2.urlopen(urls[j]).read() #changes which movie displayed 
    info = json.loads(source.decode('utf-8'))
    revURL='?review_type=all&page_limit=20&page=1&country=us&apikey='
    keys1=info.keys() #keys for highest level dictionary
  
    if len(info[keys1[0]])==0:
        df2.iloc[j] = ['NA']
    
    else:
        
        keys2=info[keys1[0]][0].keys() #keys for secondary level dictionary
        keys3=info[keys1[0]][0][keys2[1]].keys() #important link keys
        reviewURL=str(info[keys1[0]][0][keys2[1]][keys3[0]])+revURL+my_key
        source2 = urllib2.urlopen(reviewURL).read()
        review = json.loads(source2.decode('utf-8'))##pulls the review
        reviewKeys=review.keys() #keys for moview review dict
    
        if len(review[reviewKeys[0]])==0:
            df2.iloc[j] = ['NA']
        
        else:
            reviewInstKeys=review[reviewKeys[0]][0].keys() #keys for each review
            x=[]
            for i in range(len(review[reviewKeys[0]])): #iterates through all the reviews for a specific movie
                x.append(str(review[reviewKeys[0]][i][reviewInstKeys[2]]))
            df2.iloc[j] = [x]
df2['Reviews_JSON']=df2['Reviews'].apply(lambda x: json.dumps(x),1)
df2.drop('Reviews', axis=1, inplace=True)
df2.to_csv('Reviews_2014.csv')

def Review_Finder(name): 
    name1=str(name)
    ReviewDF2=pd.read_csv('/Users/patrickkuiper/Desktop/Academic/AC209/Reviews_2014.csv')
    ReviewDF=ReviewDF2.set_index('Name')
    ReviewSTR=json.loads(ReviewDF.loc[name1][0])
    Movie_DF=pd.DataFrame(ReviewSTR,columns=[name1])
    return Movie_DF
