##compute the number of businesses that users have reviewed 
###and plot the cmf distribution of the the number of users 
##x-axis represent the number of business, y-axis represent the percentage of the users who reviewed less than the x value
import simplejson as json
import datetime
import time
import matplotlib.pyplot as plt
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import numpy as np
from scipy import stats
from scipy.stats import chi2_contingency

def string_toYearMonth(string):
	return datetime.datetime.strptime(string[0:7], "%Y-%m").date()

def monthDiff(timeDate1, timeDate2):
	return (timeDate1.year-timeDate2.year)*12 + (timeDate1.month-timeDate2.month)

###load user info in userData as userData {user:[friend]}
def loadUser():
	userData={}
	userYearData={}
	userList = []
	
	userNum = 0
	userFile =  "../../dataset/user.json"
	with open(userFile) as f:
		for line in f:
			userJson = json.loads(line)
			
			friend = userJson["friends"]	
			user = userJson["user"]
			sinceTime = string_toYearMonth(userJson["sinceTime"])
			
			userYearData.setdefault(user, sinceTime)
			userYearData[user] = sinceTime
		
			userData.setdefault(user, [])
			
			if friend:
				for f in friend:	
					userData[user].append(f)
			
				userList.append(user)
			userNum += 1
	
	print "load Friend"
	print "total userNum %d"%userNum
	return (userData, userYearData, userNum, userList)
	
####load review as format:{user:{business:reviewTime}}
def loadReview():
	reviewData = {}
	
	reviewFile =  "../../dataset/review.json"
	with open(reviewFile) as f:
		for line in f:
			reviewJson = json.loads(line)
			
			business = reviewJson["business"]
			user = reviewJson["user"]
			reviewTime = string_toYearMonth(reviewJson["date"])
			
			reviewData.setdefault(user, {})
			reviewData[user].setdefault(business, reviewTime)
			reviewData[user][business] = (reviewTime)
		
	return (reviewData)	

####selectBusiness which is a list contains the sequence number selected
def randomBusiness(totalNum, randomNum):
	business = [i for i in range(totalNum)]
	selectBusiness = []
	for i in range(randomNum):
		k = np.random.randint(0, totalNum-i)+i
		temp = business[i]
		business[i] = business[k]
		business[k] = temp
		selectBusiness.append(business[i])
	return selectBusiness

#####from selectBusiness(a list)get the sequence number 
###store the business_id into selectBusinessList.
def randomSelectBusiness(reviewList, selectBusinessNum):
	businessSet = set(reviewList)
	businessLen = len(businessSet)
	
	#print businessLen
	
	if businessLen < selectBusinessNum:
		selectBusinessList = reviewList
	else:
		selectBusiness = randomBusiness(businessLen, selectBusinessNum)
		selectBusinessList = [reviewList[i] for i in selectBusiness]
	return selectBusinessList
	
def statisticAttribute(numList):
	maxNum = max(numList)
	minNum = min(numList)
	
	meanNum = np.mean(numList)

	varNum = np.var(numList) 
	
	print "max value %d min value %d mean %d var %f"%(maxNum, minNum, meanNum, varNum)
	
	keyPercentile = 90
	keyValue = np.percentile(numList, keyPercentile)
	return (keyValue, float(keyPercentile)/100)
	

##plot historgram for the numList
def plotHist(numList):
	plt.hist(numList, bins=300,normed=True)
	#plt.xlim()
	plt.show()

###timeDeltaList stores the timeDelta that two users review the commonBusiness
def mainFunction():
	(userData, userYearData, userNum, userList) = loadUser()
	reviewData = loadReview()

	userReview_numList = list()
	for user in reviewData.keys():
		userBusiLen = len(reviewData[user].keys())
		userReview_numList.append(userBusiLen)
	
	(keyValue, keyPercentile) = statisticAttribute(userReview_numList)
	print keyValue, keyPercentile
	(cdf_x, cdf_y)=get_CDF(userReview_numList)
	plot_result(cdf_x, cdf_y, keyValue, keyPercentile)
		
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
	