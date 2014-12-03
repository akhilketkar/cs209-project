__author__ = 'Akhil'

import json
import urllib2
import pandas as pd
from datetime import datetime as dt
import time

apiKey = "u8j7q6zesvbf2mb44abmhfdp"
apiSuffix = "?apikey=" + apiKey
pageLimitSuffix = "&page_limit="
querySuffix ="&q="

def getRTReviewDataForMovie(row):
    max_results = 50
    symbolsToReplace = ["&",":"]
    outDict = {"Name":[],"Rank":[],"Critic":[],"Date":[],"Freshness":[],\
                 "Publication":[],"Quote":[],"Original_Score":[]}

    if len(str(row.RevLink)) < len("http://api.rottentomatoes.com/api/public/v1.0/movies/771041731/"):
        print "No review link found for", row.Name
        return pd.DataFrame(outDict)

    queryUrl = str(row.RevLink)
    strNameToUse = row.Name.replace(" ","+")
    for s in symbolsToReplace:
        strNameToUse = strNameToUse.replace(s,"")
    finalUrl = queryUrl+apiSuffix+pageLimitSuffix+str(50)

    time.sleep(1.)
    try:
        response = urllib2.urlopen(finalUrl)
    except urllib2.HTTPError, e:
        print e.fp.read()

    jasonText = json.loads(response.read())
    numResults = jasonText["total"]
    print "Found", numResults, " reviews for",row.Name

    outDict = {"Name":[],"Rank":[],"Critic":[],"Date":[],"Freshness":[],\
                 "Publication":[],"Quote":[],"Original_Score":[]}

    for i in range(min(numResults,max_results)):
        review = jasonText["reviews"][i]

        for key in outDict.keys():
            if key.lower() in review.keys(): outDict[key].append(review[key.lower()])
            elif key == "Name": outDict["Name"].append(row.Name)
            elif key == "Rank": outDict["Rank"].append(row.Rank)
            else: outDict[key].append("N/A")

    # for key,value in outDict.items():
    #     print key,len(value)
    return pd.DataFrame(outDict)

# Load list of movies and get RT data for them

min_year = 2009
month = 13
data = pd.read_csv("movieRTAndBudgetData.csv")
data["ReleaseDate"] = data["ReleaseDate"].apply(lambda x: dt.strptime(x,"%Y-%m-%d").date())

# subset data and query Rotten Tomatoes
data2 = data[(data["Year"] > min_year) & (data["Month"] < month)]

data3 = pd.DataFrame()
for index,row in data2.iterrows():
    data3 = pd.concat((data3,getRTReviewDataForMovie(row)),axis=0)

print data3.head()
data3.to_csv("RTReviews.csv",encoding="utf-8",index=False)

# get dataset for pat
revDict = {"Name":[],"Reviews_JSON":[]}
groupedData3 = data3.groupby("Name")

for name,group in groupedData3:
    revList =[r for r in group["Quote"]]

    revDict["Name"].append(name)
    revDict["Reviews_JSON"].append(json.dumps(revList))

data5 = pd.DataFrame(revDict)
data5.to_csv("movieReviewsPat.csv",encoding="utf-8",index=False)

# merge with numbers data and save
data4 = pd.merge(data3,data)
data4.to_csv("movieRTAndBudgetDataAndReviews.csv",encoding="utf-8",index=False)
