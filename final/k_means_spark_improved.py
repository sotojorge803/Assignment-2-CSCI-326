import boto3
import sys
from pyspark.sql import SparkSession
import random
import math
import statistics
import random

'''
Authors: Jackson Jacobs and Jorge Soto-Ventura
Spark version of k_means.py
'''

def convertToFloat(point):
    out= []
    for c in point:
        out.append(float(c))
    return out

inputFileName= sys.argv[1]
k= int(sys.argv[2])
#Initialize the sparksession
session= SparkSession.builder.appName("K_Means").getOrCreate()
context= session.sparkContext
data = context.textFile(inputFileName)
lower = 1
upper= 5
numbers=data.map(lambda line: line.strip().split(","))
numbers= numbers.map(convertToFloat)
centers= []

largestStartingDistance= 0
def startingCenters(point):
    point= point[lower:upper+1]
    pointToCenterDistance= -1
    for currentCenter in centers:
        if point == currentCenter:
            return (point, pointToCenterDistance)
        distance= ((currentCenter[0]- point[0])**2) + ((currentCenter[1] - point[1]) **2)  + ((currentCenter[2] - point[2]) **2) + ((currentCenter[3] - point[3]) **2) + ((currentCenter[\
4] - point[4]) **2)
        if pointToCenterDistance > distance or pointToCenterDistance == -1:
            pointToCenterDistance= distance
            largestStartingDistance= pointToCenterDistance
    return (point, pointToCenterDistance)  

centers.append(numbers.takeSample(withReplacement=False, num=1)[0][lower:upper+1])

while len(centers) != k:
    start= numbers.map(startingCenters)
    ourRandom= random.randint(0, largestStartingDistance)
    check= True
    while check:
        randomPoint= start.takeSample(withReplacement=False, num=1)
        if ourRandom < randomPoint[0][1]:
            centers.append(randomPoint[0][0])
            check= False

print(centers)

#Function to seperate datapoints
def findCenters(point): #Finds the closest centers for the points
    point= point[lower:upper+1]
    pointToCenterDistance= -1
    for currentCenter in centers:
        if currentCenter == point:
            point.append(1)
            return (tuple(currentCenter), point)
        currentDistance= math.sqrt(((currentCenter[0]- point[0])**2) + ((currentCenter[1] - point[1]) **2)  + ((currentCenter[2] - point[2]) **2) + ((currentCenter[3] - point[3]) **2) + ((currentCenter[4] - point[4]) **2)) # Get the current distance
        if pointToCenterDistance > currentDistance or pointToCenterDistance == -1: # Finds the point's closest center
            pointToCenterDistance= currentDistance
            center= currentCenter
    point.append(1)
    return (tuple(center), point)

def kMeansSum(p1, p2):
    out= []
    for x in range(6):
        newCorValue= p1[x] + p2[x]
        out.append(newCorValue)
    return out

def totalPoints(p):
    out= []
    point= p[1]
    total= point[5]
    for i in range(5):
        c= point[i]
        avg= c/total
        out.append(avg)
    return out

centersList= numbers.map(findCenters)
centersList= centersList.reduceByKey(kMeansSum)
centersList= centersList.map(totalPoints)
newList=centersList
unNestedList=[]
newList=newList.collect()
for x in range(5):
    unNestedList.append(newList[x])
while unNestedList != centers:
    centers = unNestedList
    newList= numbers.map(findCenters)
    newList= newList.reduceByKey(kMeansSum)
    newList= newList.map(totalPoints)
    unNestedList= []
    newList= newList.collect()
    for x in range(5):
        unNestedList.append(newList[x])
else:
    print("Center:", newList)
    print("OldCenters:", centers)
    print("done")
    

