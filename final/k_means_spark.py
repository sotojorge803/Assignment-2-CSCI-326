import boto3
import sys
from pyspark.sql import SparkSession
import random
import math
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
outputFileName= sys.argv[2]
k= int(sys.argv[3])

#Initialize the sparksession
session= SparkSession.builder.appName("K_Means").getOrCreate()
context= session.sparkContext
data = context.textFile(inputFileName)
lower = 1
upper= 5
numbers=data.map(lambda line: line.strip().split(","))
numbers= numbers.map(convertToFloat)
centers= []

def startingCenters(point):
    point= point[lower:upper+1]
    pointToCenterDistance= -1
    for currentCenter in centers:
        if point == currentCenter:
            return (point, 0)
        distance= ((currentCenter[0]- point[0])**2) + ((currentCenter[1] - point[1]) **2)  + ((currentCenter[2] - point[2]) **2) + ((currentCenter[3] - point[3]) **2) + ((currentCenter[4] - point[4]) **2)
        if pointToCenterDistance > distance or pointToCenterDistance == -1:
            pointToCenterDistance= distance
    ourRandom= random.uniform(0.0, 1.0)
    weight= ourRandom * pointToCenterDistance
    return (point, weight)

centers.append(numbers.takeSample(withReplacement=False, num= 1)[0][lower:upper + 1])

while len(centers) != k: 
    start= numbers.map(startingCenters)
    pick= start.takeOrdered(1, key= lambda x: -x[1])
    pick= pick[0][0]
    centers.append(pick)
print(centers)
'''
Old weighted random choosing
while len(centers)!= k:
    start= numbers.map(startingCenters)
    topBound= start.takeOrdered(1, key= lambda x: -x[1])[0][1]
    print(topBound)
    ourRandom= random.randint(0, int(topBound)-1)
    print(ourRandom)
    check= True
    while check:
        randomPoint= start.takeSample(withReplacement=False, num =1)
        print(ourRandom, randomPoint[0][1])
        if ourRandom < randomPoint[0][1]:
            print("jere")
            centers.append(randomPoint[0][0])
            check= False
        
'''

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
    for x in range(len(p2)):
        newCorValue= p1[x] + p2[x]
        out.append(newCorValue)
    return out

def findCenterTotal(p1,p2):
    out=[]
    for x in range(2):
        newValue= p1[x] + p2[x]
        out.append(newValue)
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

def totalDigits(p):
    values= p[1]
    return values[0]/values[1]

def objective(point):
    point= point[lower:upper+1]
    pointToCenterDistance= 0
    newTotal= []
    for currentCenter in centers:
        #print(currentCenter)
        #if currentCenter == point:
            #newTotal.append(0)
            #newTotal.append(1)
            #return (tuple(currentCenter), newTotal)
        currentDistance= ((currentCenter[0]- point[0])**2) + ((currentCenter[1] - point[1]) **2)  + ((currentCenter[2] - point[2]) **2) + ((currentCenter[3] - point[3]) **2) + ((currentCenter[4] - point[4]) **2) # Get the current distance
        if pointToCenterDistance > currentDistance or pointToCenterDistance == 0: # Finds the point's closest center
            pointToCenterDistance= currentDistance
            ourCenter= currentCenter
    newTotal.append(pointToCenterDistance)
    newTotal.append(1)
        #print(ourCenter)
    return (tuple(ourCenter), newTotal)
    
centersList= numbers.map(findCenters)
centersList= centersList.reduceByKey(kMeansSum)
#print(centersList.collect())
centersList= centersList.map(totalPoints)
newList=centersList
unNestedList=[]
newList=newList.collect()
for x in range(k):
    unNestedList.append(newList[x])
while unNestedList != centers:
    centers = unNestedList
    newList= numbers.map(findCenters)
    newList= newList.reduceByKey(kMeansSum)
    newList= newList.map(totalPoints)
    unNestedList= []
    newList= newList.collect()
    for x in range(k):
        unNestedList.append(newList[x])
centers= unNestedList
#print("centers",centers)
finalList= numbers.map(objective)
#print(finalList.collect())
finalList= finalList.reduceByKey(findCenterTotal)
#print("Final:", finalList.collect())
finalList= finalList.map(totalDigits)
finalList= finalList.collect()
#print("Center:", newList)
#print("OldCenters:", centers)
finalTotal= 0
for dist in finalList:
    finalTotal+=dist
finalAvg= finalTotal/k
#print("Average Sq:", finalList)
#print(finalAvg)

finalString= "Average Sq: " + str(finalList)
s3= boto3.resource("s3")
obj= s3.Object("jacobsj-326", outputFileName)
obj.put(Body=finalString)

