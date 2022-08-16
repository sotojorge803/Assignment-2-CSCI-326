import sys
import boto3
from pyspark.sql import SparkSession
import random
import math

'''

Authors: Jackson Jacobs and Jorge Soto-Ventura

K center algorithm in spark
'''

def convertToFloat(point):
    out=[]
    for c in point:
        out.append(float(c))
    return out

inputFileName= sys.argv[1]
outputFileName= sys.argv[2]
k= int(sys.argv[3])
lower= 11
upper= 15
centers= []

session= SparkSession.builder.appName("k_center").getOrCreate()
context= session.sparkContext

data= context.textFile(inputFileName)

numbers= data.map(lambda line: line.strip().split(","))
numbers= numbers.map(convertToFloat)
centers.append(numbers.takeSample(withReplacement=False, num=1)[0][lower:upper+1]) # Sets the centers list to our randomly sampled row, specifically the columns we need

# Takes a point as input and returns the point and its distance to its closest center currently stored
def findClosestCenter(point):
    #print(point)
    point= point[lower:upper+1]
    #print(point)
    smallestDistance= 0
    for center in centers:
        if center == point:
            return (point, 0.0)
        distance= math.sqrt(((center[0] - point[0]) **2) + ((center[1] - point[1]) ** 2) + ((center[2] - point[2]) **2) + ((center[3] - point[3]) ** 2) + ((center[4] - point[4]) ** 2))
        if smallestDistance > distance or smallestDistance == 0:
            smallestDistance= distance
    return (point, smallestDistance)

while len(centers) < k:
    farthestPoints= numbers.map(findClosestCenter)
    farthestPoint= farthestPoints.takeOrdered(1, key= lambda x: -x[1])
    centers.append(farthestPoint[0][0])

objective= numbers.map(findClosestCenter)
objective= objective.takeOrdered(1, key= lambda x: -x[1])

#print(centers)
#print(objective)

finalString= str(centers) + "\nMax distance: " + str(objective) 
s3= boto3.resource("s3")
obj= s3.Object("jacobsj-326", outputFileName)
obj.put(Body=finalString)
