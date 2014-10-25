###new function:shuffling the reviewing time and debug the program
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
		
		if bNum > 50:
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

def increMonth(baseMonth):
	return baseMonth+relativedelta(months=+1)
	
###cut part of the dict and sort the dict
def SortDict_Time(timeValDict, userInfo):
	sortedTimeValDict = {}
	timeList = sorted(timeValDict)
	timeSet = set(timeList)
	
	timeUserDict_oneBiz = {}##{time:[user]}
	periodList = []
	timeUserLenDict = {}
	
	WList_oneBiz = [] ##w in the paper
	tempWList_oneBiz = []
	WSet_oneBiz = set()
	
	
	monthRange = 18
	if(monthRange > len(timeList)):
		monthRange = len(timeList)
	
	monthTime = timeList[0]
	
	for i in range(monthRange):
		periodList.append(monthTime)

		if monthTime in timeSet:
			sortedTimeValDict.setdefault(monthTime, [])
			timeUserLenDict.setdefault(monthTime, 0)
			
			reviewUserList = timeValDict[monthTime]
			reviewUserSet = set(reviewUserList)
			
			reviewUserSet = reviewUserSet.difference(WSet_oneBiz)
			reviewUserList = list(reviewUserSet)
			sortedTimeValDict[monthTime] =  reviewUserList
			
			timeUserLenDict[monthTime] = len(reviewUserList)
			
			WSet_oneBiz = WSet_oneBiz.union(reviewUserSet)
		
		monthTime = increMonth(monthTime)
	WList_oneBiz = list(WSet_oneBiz)
	tempWList_oneBiz = list(WSet_oneBiz)

	for t in periodList:
		for user in tempWList_oneBiz:
			uSinceTime = userInfo[user]["sinceTime"]
			if (monthDiff(uSinceTime, t)<=0):
				timeUserDict_oneBiz.setdefault(t, [])
				timeUserDict_oneBiz[t].append(user)
				tempWList_oneBiz.remove(user)	
		
	return (sortedTimeValDict, periodList, WList_oneBiz, timeUserDict_oneBiz, timeUserLenDict)
	
###update the userInfo: "reviewTime", "active" for a business
def UpdateUserInfo_oneBiz(userInfo, timeReviewerDict_oneBiz, selectBusiness):
	repeatReviewUserSet = set()
	for t in timeReviewerDict_oneBiz.keys():
		reviewUserList = timeReviewerDict_oneBiz[t]
		reviewUserSet = set(reviewUserList)
		
		for u in reviewUserSet:
			preActive = userInfo[u]["active"] 
			if(preActive == 1):
				continue
			else:
				userInfo[u]["active"] = 1
				userInfo[u]["reviewTime"] = t

##timeReviewerDict_oneBiz {time:[reviewer]}
def ResetUserInfo_oneBiz(userInfo, timeReviewerDict_oneBiz):
	defaultReviewTime = string_toYearMonth('2015-01')
	defaultActive = 0
	
	for t in timeReviewerDict_oneBiz.keys():
		reviewUserSet = set(timeReviewerDict_oneBiz[t])
		for u in reviewUserSet:
			userInfo[u]["reviewTime"] = defaultReviewTime
			userInfo[u]["active"] = defaultActive	
	
def UpdateTimeReviewer_allBiz(reviewData, selectBusiness, timeReviewerDict_oneBiz):
	for t in timeReviewerDict_oneBiz.keys():
		reviewUserList = timeReviewerDict_oneBiz[t]
		reviewData[selectBusiness][t] = reviewUserList

def ResetTimeReviewer_allBiz(reviewData, selectBusiness, timeReviewerDict_oneBiz):
	for t in timeReviewerDict_oneBiz.keys():
		reviewUserList = timeReviewerDict_oneBiz[t]
		reviewData[selectBusiness][t] = reviewUserList
			
def compute_oneBiz(userInfo, selectBusiness, reviewData):
	timereviewerDict_allBiz = dict(reviewData)
	reviewDict_oneBiz = timereviewerDict_allBiz[selectBusiness]
	(timeReviewerDict_oneBiz, periodList, WList_oneBiz, timeUserDict_oneBiz, timeUserLenDict) = SortDict_Time(reviewDict_oneBiz, userInfo)
	
	###before permute
	UpdateUserInfo_oneBiz(userInfo, timeReviewerDict_oneBiz, selectBusiness)

	(LR_coef, LR_intercept) = LR_oneBiz(periodList, userInfo, timereviewerDict_allBiz)
	ResetUserInfo_oneBiz(userInfo, timeReviewerDict_oneBiz)
	
	###permuteTime
	permute_timeReviewerDict_oneBiz = permuteTime(timeReviewerDict_oneBiz, timeUserDict_oneBiz, periodList, timeUserLenDict)
		
	UpdateUserInfo_oneBiz(userInfo, permute_timeReviewerDict_oneBiz, selectBusiness)
	
	UpdateTimeReviewer_allBiz(timereviewerDict_allBiz, selectBusiness, permute_timeReviewerDict_oneBiz)
	
	(LR_coef2, LR_intercept2) = LR_oneBiz(periodList, userInfo, timereviewerDict_allBiz)
	ResetUserInfo_oneBiz(userInfo, permute_timeReviewerDict_oneBiz)
	ResetTimeReviewer_allBiz(timereviewerDict_allBiz, selectBusiness, timeReviewerDict_oneBiz)
		
	return (LR_coef, LR_coef2)
			
def LR_oneBiz(periodList, userInfo, reviewData):
	R = 10
	Y = [0 for i in range(R+2)]
	N = [0 for i in range(R+2)]
	feature = []
	output = []
	activeZeroSum = 0
	unactiveZeroSum = 0
	
	positive = 0
	negative = 0
	totalReviewUserSet = set()
	
	for t in periodList:
		#print t
		activeUserSet = set()
		reviewUserSet = set()
		raw_reviewUserSet = set()
		
		###fix bugs: the reviewUserList_oneBiz does not change
		for b in reviewData.keys():
			if(reviewData[b].has_key(t)):	
				raw_reviewUserSet = raw_reviewUserSet.union(set(reviewData[b][t]))
			
		reviewUserSet = raw_reviewUserSet
		totalReviewUserSet=totalReviewUserSet.union(reviewUserSet)

		for u in totalReviewUserSet:
			uReviewTime = userInfo[u]["reviewTime"]
			uActive = userInfo[u]["active"]
			
			if(uActive == 1):
				if (uReviewTime == t):
					uActiveFriendSum = activeFriend_Sum(u, userInfo, t)
					output.append(uActive)
					
					positive += 1
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
				negative += 1
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
	
	#print "positive %d negative %d"%(positive, negative)
	(LR_coef, LR_intercept) = LR_result(feature, output)
	
	return (LR_coef, LR_intercept)
	
def LR_result(x, y):
	#print x
	model = LogisticRegression()

	x_feature = [[math.log(i+1)] for i in x]
	
	model = model.fit(x_feature, y)
	print model.score(x_feature, y)
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

def permuteTime(timeReviewerDict_oneBiz, timeUserDict_oneBiz, periodList, timeUserLenDict):
	permute_timeReviewerDict_oneBiz = {}
	
	totalSinceUserSet = set()
	for t in periodList:
		##todo
		selectReviewerSum = 0
		if timeUserLenDict.has_key(t):
			selectReviewerSum = timeUserLenDict[t]
		
		sinceUserSet = set()
		if timeUserDict_oneBiz.has_key(t):
			sinceUserList = timeUserDict_oneBiz[t]
			sinceUserSet = set(sinceUserList)
			
		totalSinceUserSet = totalSinceUserSet.union(sinceUserSet)
		
		selectUserList = randomSelectBusiness(list(totalSinceUserSet), selectReviewerSum)
		selectUserSet = set(selectUserList)
		
		permute_timeReviewerDict_oneBiz.setdefault(t, [])
		permute_timeReviewerDict_oneBiz[t] = selectUserList
		
		totalSinceUserSet = totalSinceUserSet.difference(selectUserSet)
	
	return permute_timeReviewerDict_oneBiz
	
def mainFunction():
	f1_result = open("coef1_result.txt", "w")
	f2_result = open("coef2_result.txt", "w")

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
	pool_args = [(userInfo, i, reviewData) for i in selectBusinessSet]
	
	pool = ThreadPool(8)
	results = pool.map(compute_oneBiz_helper, pool_args)
	
	# results = []
	
	# for i in range(selectBusinessNum):
		# selectBusiness = selectBusinessList[i]
		# reviewData_allBiz = dict(reviewData)
		# (LR_coef, LR_coef2) = compute_oneBiz(userInfo, selectBusiness, reviewData_allBiz)
		
		# results.append((LR_coef, LR_coef2))

	for (LR_coef, LR_coef2) in results:
		f1_result.write("%s\n"%LR_coef)
		f2_result.write("%s\n"%LR_coef2)
	
	endTime = datetime.datetime.now()
	timeIntervals = (endTime-beginTime).seconds
	print "time interval %s"%timeIntervals
	f1_result.write("time interval %s"%timeIntervals)
	f2_result.write("time interval %s"%timeIntervals)
	f1_result.close()
	f2_result.close()

mainFunction()



