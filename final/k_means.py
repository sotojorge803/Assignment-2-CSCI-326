import sys
import math
import random
from statistics import mean
'''
Authors: Jackson Jacobs and Jorge Soto-Ventura

Implements the k-means algo.
'''

inputFileName= sys.argv[1]
inputFile= open("../datasets/" + inputFileName, "r")

allPoints= []
centers= []
oldcenters=[]
delAllPoints=[]
k= 5
counter=0
closestCenterList= [x * [] for x in range(k)]
for line in inputFile:
    word=line.split()
    x= int(word[0])
    y= int(word[1])
    allPoints.append([x, y])
inputFile.close()
delAllPoints= allPoints.copy()
centersChanged= True
#Append a random center
for x in range(k):
    i= random.randint(0, len(allPoints)-1)
    centers.append(allPoints[i])
    delAllPoints.remove(delAllPoints[i])
print(centers)
pointToCenterDistance=0 # The distance between this point and the closest center
while centersChanged == True:
    counter+=1
    if not counter % 100:
        print("Counter", counter)
        print("Centers", centers)
    oldCenters= centers.copy()
    for currentPoint in allPoints:
        currentList=0
        currentIndex=0
        pointToCenterDistance=-1 # Initialize to 0 each time we go to the next point
        for currentCenter in centers:
            currentDistance= math.sqrt(((currentCenter[0]- currentPoint[0])**2) + ((currentCenter[1] - currentPoint[1])**2)) # Get the current distance
            if pointToCenterDistance > currentDistance or pointToCenterDistance == -1: # Finds the point's closest center
                pointToCenterDistance= currentDistance
                currentList= currentIndex
            currentIndex+=1
        closestCenterList[currentList].append(currentPoint) #Appends the point to a nested list corresponding to the same index as the center in centers
    xCor=0
    yCor=0
    total=0
    for x in range(len(closestCenterList)): #Finds the length of closestCenter List could also just use k
        for y in range(len(closestCenterList[x])): #Find length of the nested list in closestCenterList[x]
            xCor+= closestCenterList[x][y][0] #Adds to X cor
            yCor+= closestCenterList[x][y][1] #Adds to Y Cor
            total+=1
        meanXCor= (xCor/total)
        meanYCor= (yCor/total)
        centers[x]= [meanXCor, meanYCor] #Adds the new mean of x and y to centers depending on the current center
    if centers != oldCenters:
        centersChanged = True #Keeps true if centers and old centers are not the same
        #print("Centers",centers)
        #print("OldCenters", oldCenters)
        closestCenterList=[x * [] for x in range(k)]
    else:
        centersChanged= False
    
#K-Means


#Print Stuff
print(centers)
