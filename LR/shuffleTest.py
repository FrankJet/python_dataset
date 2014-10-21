####new function:  filter some businesses which have been reviewed by more than 10 users in the period
###R is set to a constant value 
###timeReviewUser is moved to filterReviewData module
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
###store timeReviewUser {time:[user]}
def loadReview():
	reviewData = {}
	reviewSum = {}

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
			
			reviewSum.setdefault(business, 0)
			reviewSum[business]  += 1
		
	return (reviewData, reviewSum)	

###filter business which has more than 10 users in the period
####businessList contains these business
def filterReviewData(reviewData, reviewSum, downMonth):

	print "review process"
	reviewSet = set()
	timeReviewUser = {}
	
	monthRange = 16
	monthList = increMonth(downMonth, monthRange)
	
	businessSet = set()
	for business in reviewData.keys():
		reviewSum = 0
		for t in monthList:
			timeReviewUser.setdefault(t, [])		
		
			if(reviewData[business].has_key(t)):
				timeReviewUser[t].extend(reviewData[business][t])
				reviewSum += len(reviewData[business][t])
		if reviewSum > 10:
			businessSet.add(business)
		
	businessList = list(businessSet)
	
	# finalBusinessList = list(finalBusinessSet)
	print "Business len %d"%len(businessList)
	print "end process"
	return (businessList, timeReviewUser)
	
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

def increMonth(baseMonth, monthRange):
	monthList = []
	nextMonth = baseMonth
	for i in range(monthRange):
		monthList.append(nextMonth)
		nextMonth = nextMonth+relativedelta(months=+1)
	return monthList

###update the userInfo: "reviewTime", "active" for a business
###get the permute_reviewBusinessData {time:[user]} for time in monthList
def update_userInfo(userInfo, reviewBusinessData, downMonth):
	upMonth = string_toYearMonth('2012-08')
	activeSum = 0
	
	pre_reviewBusinessData = {}
	repeatReviewUserSet = set()
	
	monthRange = 16
	monthList = increMonth(downMonth, monthRange)
	
	totalMonthRange = 130
	totalDownMonth = string_toYearMonth('2004-01')
	totalMonthList = increMonth(totalDownMonth, totalMonthRange)

	for t in totalMonthList:
		reviewUserList = reviewBusinessData[t]
		reviewUserSet = set(reviewUserList)
		
		if t in monthList:
			print "time %s"%t
			
		pre_reviewBusinessData.setdefault(t, [])
		pre_reviewBusinessData[t] = reviewUserList
			
		for u in reviewUserSet:
			preReviewTime = userInfo[u]["reviewTime"]
			if(monthDiff(preReviewTime, upMonth)<0):
				repeatReviewUserSet.add(u)
			else:
				if(betweenTime(t, downMonth, upMonth)==True):
					activeSum += 1
				userInfo[u]["active"] = 1
				userInfo[u]["reviewTime"] = t
	
	#print "active user sum %d"%activeSum
	#print "repeat user sum %d"%len(repeatReviewUserSet)
	return (repeatReviewUserSet, pre_reviewBusinessData)
	
def permute_updateUserInfo(userInfo, permute_reviewBusinessData, pre_reviewBusinessData, downMonth):
	upMonth = string_toYearMonth('2012-08')
	activeSum = 0
	defaultReviewTime = string_toYearMonth('2015-01')
	
	for t in pre_reviewBusinessData.keys():
		reviewUserList = pre_reviewBusinessData[t]
		reviewUserSet = set(reviewUserList)
		
		for u in reviewUserSet:
			userInfo[u]["active"] = 0
			userInfo[u]["reviewTime"] = defaultReviewTime
	
	for t in permute_reviewBusinessData.keys():
		reviewUserList = permute_reviewBusinessData[t]
		reviewUserSet = set(reviewUserList)
		
		for u in reviewUserSet:
			userInfo[u]["active"] = 1
			userInfo[u]["reviewTime"] = t
	
	#print "active user sum %d"%activeSum
	#print "repeat user sum %d"%len(repeatReviewUserSet)
	
###active user 
def LR_user(userInfo, repeatReviewUserSet, timeReviewUser, downMonth):
	R = 3
	Y = [0 for i in range(R+2)]
	N = [0 for i in range(R+2)]
	feature = []
	output = []
	
	positive = 0
	negative = 0
	activeZeroSum = 0
	unactiveZeroSum = 0
	
	totalReviewUserSet = set()

	monthRange = 16
	monthList = increMonth(downMonth, monthRange)
	for t in monthList:
		activeUserSet = set()
		reviewUserSet = set()
		
		if(timeReviewUser.has_key(t)):	
			raw_reviewUserSet = set(timeReviewUser[t])
			
		reviewUserSet = raw_reviewUserSet.difference(repeatReviewUserSet)
		totalReviewUserSet=totalReviewUserSet.union(reviewUserSet)

		for u in totalReviewUserSet:
			uReviewTime = userInfo[u]["reviewTime"]
			uActive = userInfo[u]["active"]
			uActiveFriendSum = activeFriend_Sum(u, userInfo, t, downMonth)
			
			if(uActive == 1):
				if (uReviewTime == t):
					positive += 1
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
				negative += 1
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
		
	#print "positive samples %d, negative sammples %d, totalsamples %d"%(positive, negative, positive+negative)
	#print "zero Friend users positive %d, negative %d"%(activeZeroSum, unactiveZeroSum)
	
	(LR_coef, LR_intercept) = LR_result(feature, output)
	
	return (LR_coef, LR_intercept)

def permute_LR_user(userInfo, repeatReviewUserSet, timeReviewUser, permute_reviewBusinessData, pre_reviewBusinessData, downMonth):
	R = 3
	Y = [0 for i in range(R+2)]
	N = [0 for i in range(R+2)]
	feature = []
	output = []
	
	positive = 0
	negative = 0
	activeZeroSum = 0
	unactiveZeroSum = 0
	
	totalReviewUserSet = set()

	monthRange = 16
	monthList = increMonth(downMonth, monthRange)
	for t in monthList:
		activeUserSet = set()
		reviewUserSet = set()
		
		
		if(timeReviewUser.has_key(t)):
			pre_reviewUserSet = set()
			permute_reviewUserSet = set()
			if pre_reviewBusinessData.has_key(t):
				pre_reviewUserSet = set(pre_reviewBusinessData[t])
			
			if permute_reviewBusinessData.has_key(t):
				permute_reviewUserSet = set(permute_reviewBusinessData[t])
			
			timeReviewUser[t] = list(set(timeReviewUser[t]).difference(pre_reviewUserSet).union(permute_reviewUserSet))
			raw_reviewUserSet = set(timeReviewUser[t])
			
		reviewUserSet = raw_reviewUserSet.difference(repeatReviewUserSet)
		totalReviewUserSet=totalReviewUserSet.union(reviewUserSet)

		for u in totalReviewUserSet:
			uActive = 0
			uReviewTime = userInfo[u]["reviewTime"]
			uActiveFriendSum = activeFriend_Sum(u, userInfo, t, downMonth)
			
			if (uReviewTime == t):
				uActive = 1
				positive += 1
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
				negative += 1
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
		
	#print "positive samples %d, negative sammples %d, totalsamples %d"%(positive, negative, positive+negative)
	#print "zero Friend users positive %d, negative %d"%(activeZeroSum, unactiveZeroSum)
	
	(LR_coef, LR_intercept) = LR_result(feature, output)
	
	return (LR_coef, LR_intercept)
	
def get_reviewUserList(reviewBusinessData, downMonth):
	reviewBusiUserList = []
	
	monthRange = 16
	monthList = increMonth(downMonth, monthRange)
	for t in monthList:
		if reviewBusinessData.has_key(t):
			reviewUserList = reviewBusinessData[t]
			reviewBusiUserList.extend(reviewUserList)
	return reviewBusiUserList

###reviewBusinessData {time:[user]} for all time
def compute_LR(userInfo, reviewBusinessData, downMonth, timeReviewUser):
	reviewBusiUserList = get_reviewUserList(reviewBusinessData, downMonth)
	
	(repeatReviewUserSet, pre_reviewBusinessData) = update_userInfo(userInfo, reviewBusinessData, downMonth)
	(LR_coef, LR_intercept) = LR_user(userInfo, repeatReviewUserSet, timeReviewUser, downMonth)

	print "here 2"
	permute_reviewBusinessData = permuteTime(reviewBusinessData, reviewBusiUserList, downMonth)
	permute_updateUserInfo(userInfo, permute_reviewBusinessData, pre_reviewBusinessData, downMonth)
	(permute_LR_coef, permute_LR_intercept) = permute_LR_user(userInfo, repeatReviewUserSet, timeReviewUser, permute_reviewBusinessData, pre_reviewBusinessData, downMonth)
	
	#if(permute_LR_coef > LR_coef)
	print "pre coef%f"%LR_coef
	print "permute coef%f"%permute_LR_coef
	
###permute_reviewBusinessData {time:[user]} for time in monthList
def permuteTime(reviewBusinessData, reviewBusiUserList, downMonth):
	monthRange = 16
	monthList = increMonth(downMonth, monthRange)
 	
	permute_reviewBusinessData = {}
	reviewBusiUserSum = len(reviewBusiUserList)
	
	for t in monthList:
		reviewUserSum = 0
		if reviewBusinessData.has_key(t):
			reviewUserList = reviewBusinessData[t]
			reviewUserSum = len(reviewUserList)
		
		selectUserList = randomSelectBusiness(reviewBusiUserList, reviewUserSum)
		
		reviewBusiUserSet = set(reviewBusiUserList).difference(set(selectUserList))
		reviewBusiUserList = list(reviewBusiUserSet)
		
		permute_reviewBusinessData.setdefault(t, [])
		permute_reviewBusinessData[t] = selectUserList
	
	return permute_reviewBusinessData
			
def LR_result(x, y):
	#print x
	model = LogisticRegression()

	x_feature = [[math.log(i+1)] for i in x]
	
	model = model.fit(x_feature, y)
	model.score(x_feature, y)
	return (model.coef_, model.intercept_)

def activeFriend_Sum(user, userInfo, uReviewTime, downMonth):
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

def compute_LR_helper(args):
	return compute_LR(*args)
	
def mainFunction(downMonth):
	f_result = open("coef_result.txt", "w")

	(userInfo, timeUserData, userSum, userList) = loadUser()
	
	(reviewData, reviewSum) = loadReview()
	(reviewList, timeReviewUser) = filterReviewData(reviewData, reviewSum, downMonth)
	
	selectBusinessNum = 1
	selectBusinessList = randomSelectBusiness(reviewList, selectBusinessNum)
	selectBusinessSet = set(selectBusinessList)
	
	beginTime = datetime.datetime.now()
	
	# positiveCoef = 0
	# negativeCoef = 0
	
	# results=[]
	# LR_args = [(userInfo, reviewData[i], downMonth, timeReviewUser) for i in selectBusinessSet]
	
	# pool = ThreadPool(8)
	# results = pool.map(compute_LR_helper, LR_args)
	
	compute_LR(userInfo, reviewData[selectBusinessList[0]], downMonth, timeReviewUser)
	
	# for (LR_coef, LR_intercept) in results:
		# if LR_coef > 0:
			# positiveCoef += 1
		# else:
			# negativeCoef += 1
		
		# #print "coef %f"%LR_coef
		# f_result.write("%s\n"%LR_coef)
	
	# f_result.write("positive coef %d, negative coef %d"%(positiveCoef, negativeCoef))
	
	endTime = datetime.datetime.now()
	timeIntervals = (endTime-beginTime).seconds
	print "time interval %s"%timeIntervals
	# f_result.write("time interval %s"%timeIntervals)
	# f_result.close()

downMonth = string_toYearMonth('2011-05')
mainFunction(downMonth)



