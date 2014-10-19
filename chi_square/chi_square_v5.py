###
###new function: permute the attributes of the users rather than the users 
##########
import simplejson as json
import datetime
import time
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import numpy as np
from scipy import stats
from scipy.stats import chi2_contingency

def string_toDatetime(string):
	return datetime.datetime.strptime(string, "%Y-%m-%d")

def string_toYear(string):
	return datetime.datetime.strptime(string[0:4], "%Y").date()

def string_toYearMonth(string):
	return datetime.datetime.strptime(string[0:7], "%Y-%m").date()

def monthDiff(timeDate1, timeDate2):
	return (timeDate1.year-timeDate2.year)*12 + (timeDate1.month-timeDate2.month)

def yearDiff(timeDate1, timeDate2):
	return (timeDate1.year-timeDate2.year)
	
##load user from json file as format {user:[friend]} to userData
##delete users who have no friends
### userYearData store {user: sinceTime}
### userList store [user]
### userNum means the total number of users
def loadUser():
	userData={}
	userYearData={}
	
	userNum = 0
	userFile =  "../../dataset/user.json"
	with open(userFile) as f:
		for line in f:
			userJson = json.loads(line)
			
			friend = userJson["friends"]	
			user = userJson["user"]
			sinceTime = string_toYear(userJson["sinceTime"])
			
			userYearData.setdefault(user, sinceTime)
			userYearData[user] = sinceTime
		
			userData.setdefault(user, [])
			
			if friend:
				for f in friend:	
					userData[user].append(f)
				
			userNum += 1
	
	print "load Friend"
	print "total userNum %d"%userNum
	return (userData, userYearData, userNum)

def filterUserNoFriend(userData):
	userFriendData = {}
	userFriendSet = set()
	
	for user in userData.keys():
		friend = userData[user]
		
		if friend:
			userFriendData.setdefault(user, [])
			for f in friend:	
				userFriendData[user].append(f)
			
			userFriendSet.add(user)
			
	userFriendList = list(userFriendSet)
	return (userFriendData, userFriendList)
"""

"""
####load review as format:{business:{user:reviewTime}}
def loadReview():
	reviewData = {}
	
	reviewFile =  "../../dataset/review.json"
	with open(reviewFile) as f:
		for line in f:
			reviewJson = json.loads(line)
			
			business = reviewJson["business"]
			user = reviewJson["user"]
			reviewTime = string_toYear(reviewJson["date"])
			
			reviewData.setdefault(business, {})
			reviewData[business].setdefault(user, reviewTime)
			reviewData[business][user] = (reviewTime)
		
	return (reviewData)	

###filter business which has more than 1000 reviews
####reviewList contains the business which has more than 1000 reviews
def filterReviewData(reviewData):

	print "review process"
	reviewSet = set()
	
	for business in reviewData.keys():
	
		bNum = len(reviewData[business].keys())
		
		if bNum > 500:
			reviewSet.add(business)
	
	reviewList = list(reviewSet)
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
	
###filter users in userFriendData, who are before baseYear, whose friends are before baseYear 
def filterUserFriendData(userFriendData, userYearData, baseYear):
	finalUserFriendData = {}
	friendSum = 0
	finalUserSet = set()
	
	for user in userFriendData.keys():
		uSinceTime = userYearData[user]
		if(yearDiff(uSinceTime, baseYear)>0):
			continue
		
		friend = userFriendData[user]
		for f in friend:
			fSinceTime = userYearData[f]
			if(yearDiff(fSinceTime, baseYear)>0):
				continue
				
			finalUserFriendData.setdefault(user, [])
			finalUserSet.add(user)
			
			finalUserFriendData[user].append(f)
			friendSum += 1
	
	friendSum = friendSum/2
	finalUserList = list(finalUserSet)
	userSum = len(finalUserList)
	
	return (finalUserFriendData, finalUserList, friendSum, userSum)

####build the user network contains the user and friends
def UserNetwork(baseYear):
	(userData, userYearData, userNum) = loadUser()
	(userFriendData, userFriendList) = filterUserNoFriend(userData)
	
	(reviewData) = loadReview()

	(finalUserFriendData, finalUserList, friendSum, userSum) = filterUserFriendData(userFriendData, userYearData, baseYear)
	
	print "final friendSum %d"%friendSum
	print "final userSum %d"%userSum
	
	return (finalUserFriendData, finalUserList, friendSum, userSum, reviewData)

####build the user network with friendship and attribute in selectYear
def UserAttributeNetwork(business, reviewData, userList, selectYear):
	userSet = set(userList)
	attributeUserSet = set()
	for user in reviewData[business].keys():
		if user not in userSet:
			continue
		
		userYear = reviewData[business][user]
		
		if (yearDiff(userYear, selectYear)>0):
			continue
		
		attributeUserSet.add(user)
	
	attributeUserList = list(attributeUserSet)
	return attributeUserList

###get the total pair of users, friends and strangers
def totalPair(userSum, friendSum):
	totalFriendPair = friendSum
	totalUserPair = (userSum*(userSum-1))/2
	totalStrangerPair = (totalUserPair-totalFriendPair)
	return (totalFriendPair, totalStrangerPair, totalUserPair)	
	
def statisticPair(attributeUserList, userFriendData, userList, totalFriendPair, totalStrangerPair):
	
	friendPair = 0
	strangerPair = 0
	
	friendPair2 = 0
	strangerPair2 = 0
	
	userSet = set(userList)
	attributeUserSet = set(attributeUserList)
	
	#print "attributeUserList len%d"%len(attributeUserSet)
	
	for user1 in attributeUserSet:
		friendSet = set(userFriendData[user1])
		
		commonSet = friendSet.intersection(attributeUserSet)
		diffSet = attributeUserSet.difference(friendSet)
		friendPair += len(commonSet)
		strangerPair += len(diffSet)
		
	friendPair = friendPair/2
	strangerPair = strangerPair/2
	
	friendPair2 = (totalFriendPair - friendPair)
	strangerPair2 = (totalStrangerPair - strangerPair)
	
	#print "C11 %d, C12 %d, C21 %d, C22 %d"%(friendPair, friendPair2, strangerPair, strangerPair2)
	return (friendPair, friendPair2, strangerPair, strangerPair2)
	
def selectAttributeNetwork(baseYear):
	selectNum = 200
	selectYear = string_toYear('2011')
	selectYear2 = string_toYear('2012')
	
	(finalUserFriendData, finalUserList, friendSum, userSum, reviewData) = UserNetwork(baseYear)
	
	reviewList = filterReviewData(reviewData)
	
	selectBusiness = randomSelectBusiness(reviewList, selectNum)

	(totalFriendPair, totalStrangerPair, totalUserPair)	= totalPair(userSum, friendSum)
	
	positiveSum = 0
	negativeSum = 0
	invalidSum = 0
	
	criticalSum = 0
	uncriticalSum = 0
	
	#bIter = 0
	beginTime = datetime.datetime.now()
	print beginTime
	
	for business in selectBusiness:
		#print "\n t"
		attributeUserList1 = UserAttributeNetwork(business, reviewData, finalUserList, selectYear)
		(friendPair, friendPair2, strangerPair, strangerPair2) = statisticPair(attributeUserList1, finalUserFriendData, finalUserList, totalFriendPair, totalStrangerPair)
	
		(kValue1, pValue1, dof1, ex1) = calChiValue(friendPair, friendPair2, strangerPair, strangerPair2)
		if(kValue1 == 0):
			invalidSum += 1
			continue
		#print "kvalue1 %f, pValue1 %f"%(kValue1, pValue1)
		
		#print "t+1"
		attributeUserList2 = UserAttributeNetwork(business, reviewData, finalUserList, selectYear2)
		(friendPair, friendPair2, strangerPair, strangerPair2) = statisticPair(attributeUserList2, finalUserFriendData, finalUserList, totalFriendPair, totalStrangerPair)
	
		(kValue_2, pValue2, dof2, ex2) = calChiValue(friendPair, friendPair2, strangerPair, strangerPair2)
		if(kValue_2 == 0):
			invalidSum += 1
			continue
		#print "before permutation kValue_2 %f, pValue2 %f"%(kValue_2, pValue2)
		
		commonAttributeUser = set(attributeUserList1).intersection(set(attributeUserList2))
		commonLen = len(commonAttributeUser)	
	#	print "commonLen %d \n"%commonLen
		
		#print "\n permute users in t+1 \n"
		iterSum = 10000
		kValue2Num = [0 for i in range(iterSum)]
		
		(diffTotalLen, diffReviewLen, diffTotalUserList) = countDiffAttribute(finalUserList, attributeUserList1, attributeUserList2)

		results = []
		pool = ThreadPool(8) ##8 is the optimal number 
		permuteAttributeArgs = [(diffTotalLen, diffReviewLen, diffTotalUserList, attributeUserList1, finalUserFriendData, finalUserList, totalFriendPair, totalStrangerPair)for i in range(iterSum)]
		results = pool.map(permuteAttribute_helper, permuteAttributeArgs)
	
		criticalVal = statisticAttribute(results, kValue_2)
		if(criticalVal>0.95):
			#print "criticalVal %f"%criticalVal
			criticalSum += 1
		else:
			#print "criticalVal %f"%criticalVal
			uncriticalSum += 1
		
		print "criticalSum %d"%criticalSum
		print "uncriticalSum %d"%uncriticalSum
		#plotOne_value(kValue2Num, kValue_2, kValue1, iterSum, 0, 0, bIter)
		#bIter += 1
	endTime = datetime.datetime.now()
	print "criticalSum %d"%criticalSum
	print "uncriticalSum %d"%uncriticalSum
	print "time interval %s"%(endTime-beginTime).seconds

	
def countDiffAttribute(totalUser, reviewUser_year1, reviewUser_year2):
	totalUserSet = set(totalUser)
	reviewUser_year1Set = set(reviewUser_year1)
	reviewUser_year2Set = set(reviewUser_year2)
	
	diffTotalUserSet = totalUserSet.difference(reviewUser_year1Set)
	diffTotalUserList = list(diffTotalUserSet)
	diffReviewUserSet = reviewUser_year2Set.difference(reviewUser_year1Set)
	
	diffTotalLen = len(diffTotalUserSet)
	diffReviewLen = len(diffReviewUserSet)
	return (diffTotalLen, diffReviewLen, diffTotalUserList)

def permuteAttribute_helper(args):
	return permuteAttribute(*args)
	
def permuteAttribute(diffTotalLen, diffReviewLen, diffTotalUserList, reviewUser_year1, finalUserFriendData, finalUserList, totalFriendPair, totalStrangerPair):
	reviewUser_year1Set=set(reviewUser_year1)
	selectUser = randomBusiness(diffTotalLen, diffReviewLen)
	
	selectUserList =  [diffTotalUserList[i] for i in selectUser]
	
	selectUserSet = set(selectUserList)
	permuteUserList = list(selectUserSet.union(reviewUser_year1Set))
	
	(friendPair, friendPair2, strangerPair, strangerPair2) = statisticPair(permuteUserList, finalUserFriendData, finalUserList, totalFriendPair, totalStrangerPair)
	
	(kValue2, pValue2, dof2, ex2) = calChiValue(friendPair, friendPair2, strangerPair, strangerPair2)
	
	return kValue2
	
def calChiValue(C11, C12, C21, C22):
	if(C11==0)or(C12==0)or(C21==0)or(C22==0):
		return (0,0,0,0)
	observed = np.array([[C11, C12], [C21, C22]])
	#print C11, C12, C21, C22
	
	(kValue, pValue, dof, ex)=chi2_contingency(observed, correction = False)
	#print "chi-square value %d"%kValue
	return (kValue, pValue, dof, ex)		
			
def mainFunction(baseYear):
	selectAttributeNetwork(baseYear)
	#plotOne(userNum, len(userNum), 0, baseYear)

def quickSort(arr, value):
	pivot = value
	criticalPercentile = float(len([x for x in arr if x < pivot])) / len(arr)
	return criticalPercentile	

def statisticAttribute(friendPairNum, comValue):
	maxFriendPair = max(friendPairNum)
	minFriendPair = min(friendPairNum)
	
	meanFriendPair = np.mean(friendPairNum)

	varFriendPair = np.var(friendPairNum) 
	
	#numDist = stats.norm(meanFriendPair, varFriendPair)
	
	criticalPercentile = quickSort(friendPairNum, comValue)
	
	print "max value %d min value %d mean %d var %f"%(maxFriendPair, minFriendPair, meanFriendPair, varFriendPair)
	
	return criticalPercentile
	
	
baseYear = string_toYear('2011')

mainFunction(baseYear)




