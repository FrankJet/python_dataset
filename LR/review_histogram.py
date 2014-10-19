####new function:  filter some businesses which have been reviewed by more than 1000 in the period
###R is set to a constant value 
import simplejson as json
import datetime
import time
import numpy as np
import math
import matplotlib.pyplot as plt
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
from dateutil.relativedelta import *
from sklearn.linear_model import LogisticRegression
from sklearn.cross_validation import train_test_split
from sklearn import metrics
from sklearn.cross_validation import cross_val_score

def string_toDatetime(string):
	return datetime.datetime.strptime(string, "%Y-%m-%d")

def string_toYear(string):
	return datetime.datetime.strptime(string[0:4], "%Y").date()

def string_toYearMonth(string):
	return datetime.datetime.strptime(string[0:7], "%Y-%m").date()

def monthDiff(timeDate1, timeDate2):
	return (timeDate1.year-timeDate2.year)*12 + (timeDate1.month-timeDate2.month)

def yearDiff(timeDate1, timeDate2):
	return (timeDate1.year-timeDate2)
	
def betweenTime(timeDate, downTime, upTime):
	if ((monthDiff(timeDate, downTime) < 0)or(monthDiff(upTime, timeDate) < 0)):
		return False
	else:
		return True

####load reviewData as format:{business:{reviewTime:[user]}}
####store reviewSum for a business as format: {business:businessSum}
###store timeReviewUser {time:[user]}
def loadReview():
	reviewData = {}
	reviewSum = {}
	timeReviewUser = {}
	
	reviewFile = "../../dataset/review.json"
	
	with open(reviewFile) as f:
		for line in f:
			reviewJson = json.loads(line)
			
			business = reviewJson["business"]
			user = reviewJson["user"]
			reviewTime = string_toYearMonth(reviewJson["date"])
			
			reviewData.setdefault(business, {})
			reviewData[business].setdefault(reviewTime, [])
			reviewData[business][reviewTime].append(user)
			
			timeReviewUser.setdefault(reviewTime, [])
			timeReviewUser[reviewTime].append(user)
			
			reviewSum.setdefault(business, 0)
			reviewSum[business]  += 1
		
	return (reviewData, reviewSum, timeReviewUser)	

###filter business which has more than 1000 reviews
####reviewList contains the business which has more than 1000 reviews
def filterReviewData(reviewSum, reviewData):
	downMonth = string_toYearMonth('2011-05')
	monthList = list()
	nextMonth = downMonth
	for i in range(16):
		monthList.append(nextMonth)
		nextMonth = increMonth(nextMonth)

	print "review process"
	#reviewSet = set()
	
	# for business in reviewSum.keys():
	
		# bNum = reviewSum[business]
		
		# if bNum > 1000:
			# reviewSet.add(business)
	
	#print "review > 1000 business sum %d"%len(reviewSet)
	businessReviewSumList = []
	finalBusinessSet = set()
	for business in reviewData.keys():
		reviewSum = 0
		for t in monthList:
			if(reviewData[business].has_key(t)):
				reviewSum += len(reviewData[business][t])
		businessReviewSumList.append(reviewSum)
		if reviewSum > 400:
			finalBusinessSet.add(business)
	
	finalBusinessList = list(finalBusinessSet)
	print "finalBusiness len %d"%len(finalBusinessList)
	print "end process"
	return (finalBusinessList, businessReviewSumList)

def increMonth(baseMonth):
	return baseMonth+relativedelta(months=+1)
	
def mainFunction():
	
	(reviewData, reviewSum, timeReviewUser)	 = loadReview()
	(reviewList, businessReviewSumList) = filterReviewData(reviewSum, reviewData)

	(keyValue, keyPercentile) = statisticAttribute(businessReviewSumList)
	print keyValue, keyPercentile
	(cdf_x, cdf_y)=get_CDF(businessReviewSumList)
	plot_result(cdf_x, cdf_y, keyValue, keyPercentile)

def statisticAttribute(numList):
	maxNum = max(numList)
	minNum = min(numList)
	
	meanNum = np.mean(numList)

	varNum = np.var(numList) 
	
	print "max value %d min value %d mean %d var %f"%(maxNum, minNum, meanNum, varNum)
	
	keyPercentile = 90
	keyValue = np.percentile(numList, keyPercentile)
	return (keyValue, float(keyPercentile)/100)	
	
def get_CDF(numList):
	print "total number of numList %d"%len(numList)
	numArray = np.asarray(numList)
	bins_num = np.arange(np.floor(numArray.min()), np.ceil(numArray.max()))
	hist, bin_edges = np.histogram(numArray, bins=bins_num, density=True)
	#print hist
	cdf = np.cumsum(hist)
	
	return (bin_edges[1:], cdf)

def plot_result(xArray, yArray, keyValue, keyPercentile):
	plt.plot(xArray, yArray)
	plt.plot([keyValue], [keyPercentile], 'ro')
	plt.show()	

	
	
mainFunction()