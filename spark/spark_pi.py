from pyspark import SparkContext
import random
import math
import boto3

'''
Authors: Jackson Jacobs and Jorge Soto-Ventura

Estimates the value of pi.
'''
context= SparkContext(appName = "FindPi", master= "local")


l=[0 for x in range(100000)]

data= context.parallelize(l)

def randomize(num):
    x= random.uniform(-1,1)
    y= random.uniform(-1,1)
    return [x,y]

def inCircle(points):
    if math.sqrt(((points[0] - 0)**2) + ((points[1] - 0)**2)) <= 1:
        return True
    else:
        return False

data= data.map(randomize)

circlePoints= data.filter(inCircle)

estimation= (circlePoints.count()/data.count()) * 4
s3= boto3.resource("s3")
obj= s3.Object("jacobsj-326", "pi_out.txt")
obj.put(str(estimation))

'''
print("Number in the square: ", data.count())
print("Number in the circle: ", circlePoints.count())
print("The estimation of pi: ", (circlePoints.count()/data.count()) * 4)
'''
