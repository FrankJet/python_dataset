####new function:  filter some businesses which have been reviewed by more than 1000 in the period
###R is set to a constant value 
###plot the distribution of the user along activeFriendSum
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
def filterReviewData(reviewData, reviewSum):

	print "review process"
	reviewSet = set()
	
	for business in reviewSum.keys():
	
		bNum = reviewSum[business]
		
		if bNum > 1000:
			reviewSet.add(business)
			
	monthList = list()
	nextMonth = downMonth
	for i in range(16):
		monthList.append(nextMonth)
		nextMonth = increMonth(nextMonth)
	
	finalBusinessSet = set()
	for business in reviewSet:
		reviewSum = 0
		for t in monthList:
			if(reviewData[business].has_key(t)):
				reviewSum += len(reviewData[business][t])
		if reviewSum > 1000:
			finalBusinessSet.add(business)
	
	finalBusinessList = list(finalBusinessSet)
	print "finalBusiness len %d"%len(finalBusinessList)
	print "end process"
	return (finalBusinessList)
	
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
	
	print businessLen
	
	if businessLen < selectBusinessNum:
		selectBusinessList = reviewList
	else:
		selectBusiness = randomBusiness(businessLen, selectBusinessNum)
		selectBusinessList = [reviewList[i] for i in selectBusiness]
	return selectBusinessList

def increMonth(baseMonth):
	return baseMonth+relativedelta(months=+1)

###update the userInfo: "reviewTime", "active" for a business
def update_userInfo(userInfo, reviewBusinessData):
	downMonth = string_toYearMonth('2011-05')
	upMonth = string_toYearMonth('2012-08')
	activeSum = 0
	
	repeatReviewUserSet = set()
	for t in reviewBusinessData.keys():
		reviewUserSet = set(reviewBusinessData[t])
		for u in reviewUserSet:
			preActive = userInfo[u]["active"] 
			if(preActive == 1):
				preReviewTime = userInfo[u]["reviewTime"]
				if(betweenTime(preReviewTime, downMonth, upMonth) == True):
					repeatReviewUserSet.add(u)
			else:
				if(betweenTime(t, downMonth, upMonth)==True):
					activeSum += 1
				userInfo[u]["active"] = 1
				userInfo[u]["reviewTime"] = t
	
	print "active user sum %d"%activeSum
	print "repeat user sum %d"%len(repeatReviewUserSet)
	return repeatReviewUserSet
	
###active user 
def LR_user(userInfo, reviewBusinessData, downMonth, timeReviewUser):
	R = 5
	Y = [0 for i in range(R+2)]
	N = [0 for i in range(R+2)]
	feature = []
	output = []
	
	positive = 0
	negative = 0
	activeZeroSum = 0
	unactiveZeroSum = 0
	
	monthList = list()
	totalReviewUserSet = set()

	repeatReviewUserSet = update_userInfo(userInfo, reviewBusinessData)
	
	nextMonth = downMonth
	for i in range(16):
		monthList.append(nextMonth)
		nextMonth = increMonth(nextMonth)
	
	for t in monthList:
		activeUserSet = set()
		reviewUserSet = set()
		
		if(timeReviewUser.has_key(t)):	
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
		
	print "positive samples %d, negative sammples %d, totalsamples %d"%(positive, negative, positive+negative)
	print "zero Friend users positive %d, negative %d"%(activeZeroSum, unactiveZeroSum)
	
	(LR_coef, LR_intercept) = LR_result(feature, output)
	
	return (LR_coef, LR_intercept, Y)
	
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
	#print "friendLen %d"%friendLen
	for f in friendSet:
		fActive = userInfo[f]["active"]
		if (fActive == 0):
			continue
		fReviewTime = userInfo[f]["reviewTime"]
		if(monthDiff(fReviewTime, uReviewTime)<0):
			activeFriendSum += 1
	#print "active%d"%activeFriendSum
	return activeFriendSum

def LR_user_helper(args):
	return LR_user(*args)
	
def mainFunction(downMonth, upMonth):
	(userInfo, timeUserData, userSum, userList) = loadUser()
	
	(reviewData, reviewSum, timeReviewUser) = loadReview()
	(reviewList) = filterReviewData(reviewData, reviewSum)
	
	selectBusinessNum = 1
	selectBusinessList = randomSelectBusiness(reviewList, selectBusinessNum)
	selectBusinessSet = set(selectBusinessList)
	
	beginTime = datetime.datetime.now()
	
	positiveCoef = 0
	negativeCoef = 0
	
	# results=[]
	# LR_args = [(userInfo, reviewData[i], downMonth, timeReviewUser) for i in selectBusinessSet]
	
	# pool = ThreadPool(8)
	# results = pool.map(LR_user_helper, LR_args)
	
	reviewBusinessData = reviewData[selectBusinessList[0]]
	
	(LR_coef, LR_intercept, LR_Y) = LR_user(userInfo, reviewBusinessData, downMonth, timeReviewUser)
	
	endTime = datetime.datetime.now()
	timeIntervals = (endTime-beginTime).seconds
	print "positive %d"%np.sum(LR_Y)
	print "time interval %s"%timeIntervals
	print "Coef %f"%LR_coef
	plotOne(LR_Y, len(LR_Y), 0, 0)

	###plot the distribution of number of review
def plotOne(num1, maxReview, minReview, baseYear):
	x = np.arange((baseYear+minReview), (baseYear+maxReview), 1)
	
	plt.figure()
	plt.plot(x, num1, 'ro')
	#plt.plot(x, num1, 'ro', x, num1, 'k')
	
	plt.xlabel("year")
	plt.ylabel("userNum")
	plt.title("UserNum By Year")
	plt.show()
	
def plotOne_value(num1, comValue, baseValue, maxReview, minReview, baseYear, i):
	x = np.arange((baseYear+minReview), (baseYear+maxReview), 1)
	
	midleX = (maxReview - minReview)/2
	plt.figure()
	#plt.plot(x, yNum, 'bo', x, yNum, 'k')
	plt.plot(x, num1, 'ro', x, num1, 'k')
	plt.plot([midleX], [comValue], 'bo')
	plt.axhline(y=baseValue, linewidth=10, color='g')
	plt.xlabel("iter")
	plt.ylabel("k-value")
	plt.title("random test")
	plt.savefig('D:/Yelp/iter_%02d.png'%i, format='png')
	
	
def plotTwo(num1, num2, maxReview, minReview, baseYear):
	x = np.arange((baseYear+minReview), (baseYear+maxReview), 1)
	
	plt.figure()
	#plt.plot(x, yNum, 'bo', x, yNum, 'k')
	plt.plot(x, num1, 'ro', x, num1, 'k')
	plt.plot(x, num2, 'bo', x, num2, 'k')
	
	plt.xlabel("year")
	plt.ylabel("userNum")
	plt.title("UserNum By Year")
	plt.show()
	
def plotThree(num1, num2, num3, maxReview, minReview, baseYear):
	x = np.arange((baseYear+minReview), (baseYear+maxReview), 1)
	
	plt.figure()
	#plt.plot(x, yNum, 'bo', x, yNum, 'k')
	#plt.plot(x, num1, 'ro')
	#plt.plot(x, num2, 'k')
	#plt.plot(x, num3, 'go')
	
	plt.plot(x, num1, 'ro', x, num1, 'k')
	plt.plot(x, num2, 'bo', x, num2,'k')
	plt.plot(x, num3, 'go', x, num3, 'k')
	
	plt.xlabel("year")
	plt.ylabel("userNum")
	plt.title("UserNum By Year")
	plt.show()

downMonth = string_toYearMonth('2011-05')
upMonth = string_toYearMonth('2012-08')
mainFunction(downMonth, upMonth)



