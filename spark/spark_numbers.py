from pyspark import SparkContext

'''
Authors: Jackson Jacobs and Jorge Soto-Ventura

Finds the min max and average of a text file.
'''

context= SparkContext(appName = "FindInfo", master= "local")

data= context.textFile("../datasets/skin_med.txt")
numbers= data.map(lambda line: line.strip().split())

def findMin(l1, l2):
    outList=[0] * len(l1)
    for i in range(len(l1)):
        num1= int(l1[i])
        num2= int(l2[i])
        if num1 < num2:
            outList[i]=num1
        else:
            outList[i]=num2

    return outList

def findMax(l1,l2):
    outList=[0] * len(l1)
    for i in range(len(l1)):
        num1= int(l1[i])
        num2= int(l2[i])
        if num1 > num2:
            outList[i]=num1
        else:
            outList[i]=num2

    return outList

def findTotal(l1,l2):
    outList=[0] * len(l1)
    for i in range(len(l1)):
        num1= int(l1[i])
        num2= int(l2[i])
        outList[i]= num1+num2
    return outList       
            
minList= numbers.reduce(findMin)
maxList= numbers.reduce(findMax)
totalList= numbers.reduce(findTotal)

i=0
avgList=[0,0,0]
for num in totalList:
   avgList[i]=num/numbers.count()
   i+=1

print(minList)
print(maxList)
print(avgList)
