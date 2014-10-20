###read the coef from the txt
##plot the cdf of coef

import numpy as np
import math
import matplotlib.pyplot as plt

def readTxtToList():
	fp_read = open("readTest.txt", "r")
	coefList = []
	
	for line in fp_read:
		readNum = line[line.find("[")+1:line.find("]")]
		coef = readNum[readNum.find("[")+1:]
		coefList.append(coef)
		
	fp_read.close()
	return coefList

def mainFunction():
	
	coefList = readTxtToList()

	(keyValue, keyPercentile) = statisticAttribute(coefList)
	print keyValue, keyPercentile
	(cdf_x, cdf_y)=get_CDF(coefList)
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

	
readTxtToList()