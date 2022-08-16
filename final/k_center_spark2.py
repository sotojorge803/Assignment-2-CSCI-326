import sys
import boto3
from pyspark.sql import SparkSession
import random
import math

'''

Authors: Jackson Jacobs and Jorge Soto-Ventura

K center algorithm in spark (for the skin dataset)
'''

def convertToInt(point):
    out=[]
    for c in point:
        out.append(int(c))
    return out

inputFileName= sys.argv[1]
outputFileName= sys.argv[2]
k= int(sys.argv[3])
centers= []

session= SparkSession.builder.appName("k_center").getOrCreate()
context= session.sparkContext

data= context.textFile(inputFileName)

numbers= data.map(lambda line: line.strip().split(" "))
numbers= numbers.map(convertToInt)
centers.append(numbers.takeSample(withReplacement=False, num=1)[0]) # Sets the centers list to our randomly sampled row, specifically the columns we need
#print(centers)
#print(numbers.collect())

# Takes a point as input and returns the point and its distance to its closest center currently stored
def findClosestCenter(point):
    smallestDistance= 0
    for center in centers:
        if center == point:
            return (point, 0)
        distance= math.sqrt(((center[0] - point[0]) **2) + ((center[1] - point[1]) ** 2) + ((center[2] - point[2]) **2))
        if smallestDistance > distance or smallestDistance == 0:
            smallestDistance= distance
    return (point, smallestDistance)

while len(centers) < k:
    farthestPoints= numbers.map(findClosestCenter)
    farthestPoint= farthestPoints.sortBy(lambda x: -x[1]).take(1)
    #print(farthestPoints.collect())
    centers.append(farthestPoint[0][0])
    #farthestPoint=[]

objective= numbers.map(findClosestCenter)
objective= objective.sortBy(lambda x: -x[1]).take(1)

#print(centers)
#print(objective)

finalString= str(centers) + "\nMax distance: " + str(objective) 
s3= boto3.resource("s3")
obj= s3.Object("jacobsj-326", outputFileName)
obj.put(Body=finalString)
