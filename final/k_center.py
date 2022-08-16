import sys
import math
import random

'''
Authors: Jackson Jacobs and Jorge Soto-Ventura

Implements the k-center algo.
'''

inputFileName= sys.argv[1]
inputFile= open("../datasets/" + inputFileName, "r")

allPoints= []
centers= []
k= 4

for line in inputFile:
    word=line.split()
    x= int(word[0])
    y= int(word[1])
    allPoints.append([x, y])
inputFile.close()

#Append a random center
i= random.randint(0, len(allPoints)-1)
centers.append(allPoints[i])
allPoints.remove(allPoints[i])

pointToCenterDistance=0 # The distance between this point and the closest center
furthestPoint=[] # The furthest point [x, y]
furthestDistance=0 # The furthest distance between a point and its closest center
while len(centers) != k:
    for currentPoint in allPoints:
        pointToCenterDistance=0 # Initialize to 0 each time we go to the next point
        for currentCenter in centers:
            currentDistance= math.sqrt(((currentCenter[0]- currentPoint[0])**2) + ((currentCenter[1] - currentPoint[1])**2)) # Get the current distance
            if pointToCenterDistance > currentDistance or pointToCenterDistance == 0: # Finds the point's closest center
                pointToCenterDistance= currentDistance
        if pointToCenterDistance > furthestDistance: # If the distance to center found for this point is the largest yet, set it to the furthest point
            furthestPoint= currentPoint
            furthestDistance= pointToCenterDistance
        
    furthestDistance=0 # At the end, we have found the furthest point from the point's closest center. We can reset furhtest distance and add the point we found to the centers
    allPoints.remove(furthestPoint)
    centers.append(furthestPoint)

maxPoint=[] # Where we will store the point that is farthest away from its center (not necessary)
maxDistance= 0 # The maximum distance between a point and its closest center
for currentPoint in allPoints:
    pointToCenterDistance= 0 # This will be reset to 0 every time we go to the next point
    for currentCenter in centers:
        currentDistance= math.sqrt(((currentCenter[0]- currentPoint[0])**2) + ((currentCenter[1] - currentPoint[1])**2)) # Get the distance
        if currentDistance < pointToCenterDistance or pointToCenterDistance == 0: # Finds the point's closest neighbor
            pointToCenterDistance= currentDistance
    if pointToCenterDistance > maxDistance: # If this points distance from its center is greater than the currently stored max distance, this new point is the max away from its center. Do for all points
        maxDistance= pointToCenterDistance
        maxPoint= currentPoint

# Print stuff
#print(maxPoint)
print(centers)
print(maxDistance)
outputFile= open("k_center_out.txt", "w")
outputFile.write(str(centers) + "\n" + str(maxDistance))
outputFile.close()
