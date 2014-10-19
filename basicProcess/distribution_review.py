##obtain the number of users who review a specific business
###count the number of businesses for a specific value of users
###plot these numbers of businesses

import simplejson as json
import datetime
import time
import matplotlib.pyplot as plt
import numpy as np

def string_toDatetime(string):
	return datetime.datetime.strptime(string, "%Y-%m-%d")

def string_toYear(string):
	return datetime.datetime.strptime(string[0:4], "%Y").date().year
	
def reviewPerYear(data, baseYear, yearTime):	
	reviewYear=[0 for i in range(yearTime)]
	for viewData in data:
		reviewDate = string_toDatetime(viewData["date"]).date();
		yearDiff = reviewDate.year-baseYear
		if yearDiff > 0 :
			reviewYear[yearDiff] += 1;		
	return reviewYear

#####
####load review as format:{business:{userId, date}}
#####	
def loadReview():
	reviewData = {}
	reviewFile = "F:\Temp\yelp\yelp_academic_dataset_review.json"
	with open(reviewFile) as f:
		for line in f:
			reviewJson = json.loads(line)
			
			business = reviewJson["business_id"]
			user = reviewJson["user_id"]
			sinceTime = string_toYear(reviewJson["date"])
			
			reviewData.setdefault(business, {})
			reviewData[business].setdefault(user, sinceTime)
			reviewData[business][user] = sinceTime
		
	return reviewData

###get the review number for every business. 
####append the number into reviewNum
###change to y-axis 
def distributionOfReview(reviewData):
	reviewNum = []

	for business in reviewData.keys():
		bNum = len(reviewData[business].keys())
		reviewNum.append(bNum)
	
	maxReview = max(reviewNum)
	minReview = min(reviewNum)
	lenReview = (maxReview - minReview+1)
	yNum = [0 for i in range(lenReview)]
	
	print maxReview, minReview
	
	fiftyNum = 0
	for i in reviewNum:
		if i >500:
			fiftyNum += 1
		yNum[i-minReview] += 1
	
	minM = min(yNum)
	maxM = max(yNum)
	
	print yNum.index(minM)
	print yNum.index(maxM)
	
	print yNum[minReview-minReview]
	print yNum[maxReview-minReview]
	print ">50 %d"%fiftyNum
	return (yNum, maxReview, minReview)

###plot the distribution of number of review
def plotReview(yNum, maxReview, minReview):
	x = np.arange(minReview, maxReview+1, 1)
	plt.figure()
	#plt.plot(x, yNum, 'bo', x, yNum, 'k')
	plt.plot(x, yNum, 'ro')
	plt.show()
	
reviewData = loadReview()
(yNum, maxReview, minReview) = distributionOfReview(reviewData)
plotReview(yNum, maxReview, minReview)

