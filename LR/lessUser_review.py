####obtain the users who have reviewed 1 business
###obtain the number of pairs of friends among these users
##obtain the number of pairs of friends among these users who have shared common business 
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

##plot historgram for the numList
def plotHist(numList):
	plt.hist(numList, bins=300,normed=True)
	#plt.xlim()
	plt.show()

###timeDeltaList stores the timeDelta that two users review the commonBusiness
def mainFunction():
	(userData, userYearData, userNum, userList) = loadUser()
	reviewData = loadReview()

	lessUserList = list()
	for user in reviewData.keys():
		userBusiLen = len(reviewData[user].keys())
		if userBusiLen < 2:
			lessUserList.append(user)
	
	lessUserList_len = len(lessUserList)
	print "lessUserList len %d"%lessUserList_len
	
	lessUserSum = 0
	pairSum = 0
	lessPairSum = 0
	lessUserSet = set(lessUserList)
	for user in lessUserSet:
		friendSet = set(userData[user])
		userFriendSet = lessUserSet.intersection(friendSet)
		
		for f in userFriendSet:
			pairSum += len(userFriendSet)
			uBusiList = reviewData[user].keys()
			fBusiList = reviewData[f].keys()
			
			commonBusiSet = set(uBusiList).intersection(set(fBusiList))
			if len(commonBusiSet) > 0:
				lessPairSum += 1
	
	print "lessPairSum %d"%(lessPairSum/2)
	print "pairSum %d"%(pairSum/2)
		
def get_CDF(numList):
	print "total number of numList %d"%len(numList)
	numArray = np.asarray(numList)
	bins_num = np.arange(np.floor(numArray.min()), np.ceil(numArray.max()))
	hist, bin_edges = np.histogram(numArray, bins=bins_num, density=True)
	#print hist
	cdf = np.cumsum(hist)
	
	plot_CDF(bin_edges[1:], cdf)

def plot_CDF(xArray, yArray):
	plt.plot(xArray, yArray)
	plt.show()
	
mainFunction()

"""
1.the result shows that there are 1341695 pairs of friends who have reviewed less than 1 business. 

2.there are 633 pairs of friends among the 1341695 pairs have common sharing business

3.there are 128970 users who have reviewed less than 1 business

"""