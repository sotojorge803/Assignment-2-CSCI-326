from pyspark.sql import SparkSession
import sys
import boto3

'''
Authors: Jackson Jacobs and Jorge Soto-Ventura

Finds the min max and average of a text file.
'''

inputFileName= sys.argv[1]

session= SparkSession.builder.appName("Numbers").getOrCreate()
context= session.sparkContext

data= context.textFile(inputFileName)

numbers= data.map(lambda line: line.strip().split(","))

def findMin(l1, l2):
    outList=[0] * 6
    for i in range(6):
        num1= float(l1[i])
        num2= float(l2[i])
        if num1 < num2:
            outList[i]=num1
        else:
            outList[i]=num2

    return outList

def findMax(l1,l2):
    outList=[0] * 6
    for i in range(6):
        num1= float(l1[i])
        num2= float(l2[i])
        if num1 > num2:
            outList[i]=num1
        else:
            outList[i]=num2

    return outList

def findTotal(l1,l2):
    outList=[0] * 6
    for i in range(6):
        num1= float(l1[i])
        num2= float(l2[i])
        outList[i]= num1+num2
    return outList       
            
minList= numbers.reduce(findMin)
maxList= numbers.reduce(findMax)
totalList= numbers.reduce(findTotal)

i=0
avgList=[0] * 6
for num in totalList:
   avgList[i]=num/numbers.count()
   i+=1
   
s3= boto3.resource("s3")
obj= s3.Object("jacobsj-326", "numbers_out.txt")
output= "Minimum: " + str(minList) + "\nMaximum: " + str(maxList) + "\nAverage: " + str(avgList)
obj.put(Body= output)
