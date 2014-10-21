###new function: statistic the beginReviewTime for a business and build the period from the beginning reviewTime.
###R is set to a constant value 
import simplejson as json
import datetime
import time
import numpy as np
import math
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
#####
###the data structure of userInfo is a list which stores the dict of a user
###userInfo {user:{"sinceTime":sinceTime, "reviewTime":reviewTime,  "active":0,1, "friends":[]}}
###reviewTime represents the first time user review the business.
###for different business, the reviewTime and active is different
###timeUserData {time:[user]}
##########
def loadUser():
	userInfo = {}
	timeUserData = {}
	
	defaultReviewTime = string_toYearMonth('2015-01')
	defaultActive = 0
	
	userSet = set()
	userSum = 0
	
	userFile = "../../dataset/user.json"
	with open(userFile) as f:
		for line in f:
			userJson = json.loads(line)
			
			user=userJson["user"]
			friend = userJson["friends"]	
			sinceTime = string_toYearMonth(userJson["sinceTime"])
			
			userInfo.setdefault(user, {})
			userInfo[user]["sinceTime"] = sinceTime
			userInfo[user]["reviewTime"] = defaultReviewTime
			userInfo[user]["active"] = defaultActive
			userInfo[user]["friends"] = []
			
			timeUserData.setdefault(sinceTime, [])
			timeUserData[sinceTime].append(user)
			
			if friend:
				for f in friend:	
					userInfo[user]["friends"].append(f)
				
			userSum += 1
			userSet.add(user)
	
	userList = list(userSet)
	
	print "load Friend"
	print "total userSum %d"%userSum
	return (userInfo, timeUserData, userSum, userList)

####load reviewData as format:{business:{reviewTime:[user]}}
####store reviewSum for a business as format: {business:businessSum}
###store timeReviewerDict_allBiz {time:[user]}
def loadReview():
	reviewData = {}
	reviewSum = {}
	timeReviewerDict_allBiz = {}
	
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
			
			timeReviewerDict_allBiz.setdefault(reviewTime, [])
			timeReviewerDict_allBiz[reviewTime].append(user)
			
			reviewSum.setdefault(business, 0)
			reviewSum[business]  += 1
		
	return (reviewData, reviewSum, timeReviewerDict_allBiz)	

###filter business which has more than 10 users in the period
####businessList contains these business
def filterReviewData(reviewData, reviewSum):

	print "review process"
	reviewSet = set()
	
	for business in reviewSum.keys():
	
		bNum = reviewSum[business]
		
		if bNum > 1000:
			reviewSet.add(business)
	reviewList = list(reviewSet)
	
	# finalBusinessList = list(finalBusinessSet)
	print "end process"
	return (reviewList)
	
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
	
	if businessLen < selectBusinessNum:
		selectBusinessList = reviewList
	else:
		selectBusiness = randomBusiness(businessLen, selectBusinessNum)
		selectBusinessList = [reviewList[i] for i in selectBusiness]
	return selectBusinessList

###cut part of the dict and sort the dict
def SortDict_Time(timeValDict):
	sortedTimeValDict = {}
	timeList = sorted(timeValDict)
	
	periodList = []
	monthRange = 16
	for i in range(monthRange):
		monthTime = timeList[i]
		sortedTimeValDict.setdefault(monthTime, [])
		sortedTimeValDict[monthTime] =  timeValDict[monthTime]
		periodList.append(i)
		
	return (sortedTimeValDict, periodList)
	
###update the userInfo: "reviewTime", "active" for a business
def UpdateUserInfo_oneBiz(userInfo, timeReviewerDict_oneBiz):
	repeatReviewUserSet = set()
	for t in timeReviewerDict_oneBiz.keys():
		reviewUserSet = set(timeReviewerDict_oneBiz[t])
		for u in reviewUserSet:
			preActive = userInfo[u]["active"] 
			if(preActive == 1):
				repeatReviewUserSet.add(u)
			else:
				userInfo[u]["active"] = 1
				userInfo[u]["reviewTime"] = t
	
	return repeatReviewUserSet

##timeReviewerDict_oneBiz {time:[reviewer]}
def ResetUserInfo_oneBiz(userInfo, timeReviewerDict_oneBiz):
	defaultReviewTime = string_toYearMonth('2015-01')
	defaultActive = 0
	
	for t in timeReviewerDict_oneBiz.keys():
		reviewUserSet = set(timeReviewerDict_oneBiz[t])
		for u in reviewUserSet:
			userInfo[u]["reviewTime"] = defaultReviewTime
			userInfo[u]["active"] = defaultActive	
	
def compute_oneBiz(userInfo, reviewDict_oneBiz, timeReviewerDict_allBiz):
	(timeReviewerDict_oneBiz, periodList) = SortDict_Time(reviewDict_oneBiz)
	
	repeatReviewUserSet = UpdateUserInfo_oneBiz(userInfo, timeReviewerDict_oneBiz)
	(LR_coef, LR_intercept) = LR_oneBiz(periodList, userInfo, timeReviewerDict_allBiz, repeatReviewUserSet)
	ResetUserInfo_oneBiz(userInfo, timeReviewerDict_oneBiz)
	
	return (LR_coef, LR_intercept)
			
def LR_oneBiz(periodList, userInfo, timeReviewerDict_allBiz, repeatReviewUserSet):
	R = 3
	Y = [0 for i in range(R+2)]
	N = [0 for i in range(R+2)]
	feature = []
	output = []
	activeZeroSum = 0
	unactiveZeroSum = 0
	
	totalReviewUserSet = set()
	
	for t in periodList:
		activeUserSet = set()
		reviewUserSet = set()
		
		if(timeReviewerDict_allBiz.has_key(t)):	
			raw_reviewUserSet = set(timeReviewerDict_allBiz[t])
			
		reviewUserSet = raw_reviewUserSet.difference(repeatReviewUserSet)
		totalReviewUserSet=totalReviewUserSet.union(reviewUserSet)

		for u in totalReviewUserSet:
			uReviewTime = userInfo[u]["reviewTime"]
			uActive = userInfo[u]["active"]
			
			if(uActive == 1):
				if (uReviewTime == t):
					uActiveFriendSum = activeFriend_Sum(u, userInfo, t)
					output.append(uActive)
					
					if(uActiveFriendSum == 0):
						activeZeroSum += 1
					
					if uActiveFriendSum > R:
						feature.append(R+1)
						Y[R+1] += 1
					else:
						feature.append(uActiveFriendSum)
						Y[uActiveFriendSum] += 1
				activeUserSet.add(u)
				
			else:
				uActiveFriendSum = activeFriend_Sum(u, userInfo, t)
				output.append(uActive)
				if(uActiveFriendSum == 0):
					unactiveZeroSum += 1
				
				if uActiveFriendSum > R:
					feature.append(R+1)
					N[R+1] += 1
				else:
					feature.append(uActiveFriendSum)
					N[uActiveFriendSum] += 1

						
		totalReviewUserSet = totalReviewUserSet.difference(activeUserSet)
	
	(LR_coef, LR_intercept) = LR_result(feature, output)
	
	return (LR_coef, LR_intercept)
	
def LR_result(x, y):
	#print x
	model = LogisticRegression()

	x_feature = [[math.log(i+1)] for i in x]
	
	model = model.fit(x_feature, y)
	model.score(x_feature, y)
	return (model.coef_, model.intercept_)

def activeFriend_Sum(user, userInfo, uReviewTime):
	friendList = userInfo[user]["friends"]
	friendSet = set(friendList)
	
	activeFriendSum = 0
	
	friendLen = len(friendSet)
	
	for f in friendSet:
		fActive = userInfo[f]["active"]
		if (fActive == 0):
			continue
		fReviewTime = userInfo[f]["reviewTime"]
		if(monthDiff(fReviewTime, uReviewTime)<0):
			activeFriendSum += 1
	#print "active%d"%activeFriendSum
	return activeFriendSum

def compute_oneBiz_helper(args):
	return compute_oneBiz(*args)
	
def mainFunction():
	f_result = open("coef_result.txt", "w")

	(userInfo, timeUserData, userSum, userList) = loadUser()
	
	(reviewData, reviewSum, timeReviewUser) = loadReview()
	(reviewList) = filterReviewData(reviewData, reviewSum)
	
	selectBusinessNum = 1
	selectBusinessList = randomSelectBusiness(reviewList, selectBusinessNum)
	selectBusinessSet = set(selectBusinessList)
	
	beginTime = datetime.datetime.now()
	
	positiveCoef = 0
	negativeCoef = 0
	
	results=[]
	pool_args = [(userInfo, reviewData[i], timeReviewerDict_allBizfor) for i in selectBusinessSet]
	
	pool = ThreadPool(8)
	results = pool.map(compute_oneBiz_helper, pool_args)
	
	for (LR_coef, LR_intercept) in results:
		if LR_coef > 0:
			positiveCoef += 1
		else:
			negativeCoef += 1
		
		#print "coef %f"%LR_coef
		f_result.write("%s\n"%LR_coef)
	
	f_result.write("positive coef %d, negative coef %d"%(positiveCoef, negativeCoef))
	
	endTime = datetime.datetime.now()
	timeIntervals = (endTime-beginTime).seconds
	print "time interval %s"%timeIntervals
	f_result.write("time interval %s"%timeIntervals)
	f_result.close()

mainFunction()



