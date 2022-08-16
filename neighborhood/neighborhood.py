import math
import os
'''
Author: Jackson Jacobs & Jorge Soto-Ventura

Finds the neighborhood of each data point.
'''

def getDistances(number):
    if number>25:
        return False
    else:
        return True

def main(file, outputFileName):
    directory=os.path.dirname(os.path.abspath(__file__))
    directory=directory.removesuffix("neighborhood")
    directory= directory + "/datasets/" + file

    file= open(directory, "r")
    xList=[]
    yList=[]
    neighborhood=[]
    distances=[]
    allNeighbors=[]
    biggestNeighbors= []
    l=[]
    finalCombined=[]

    #Creates two seperate lists for X and Y variables
    for line in file:
        word=line.split()
        x= int(word[0])
        y= int(word[1])
        xList.append(x)
        yList.append(y)
    file.close()

    points=list(zip(xList, yList))

    # Creates global variables for the for loops in order to move through each X Y list
    compareIndex=0
    currentIndex=0
    totalPoints=0
    largestNeighborhood=0

    # Finds the current point in order to compare to other points
    for current in points:
        currentPoint= points[currentIndex]
        # Compares current point to all other point to find cover.
        for point in points:
            nextPoint= points[compareIndex]
            distances.append(math.sqrt(((nextPoint[0]- currentPoint[0])**2) + ((nextPoint[1] - currentPoint[1]))**2))
            compareIndex+=1


        neighborhood=list(filter(getDistances, distances)) #gets the current neighborhood's distances (not points)
        
        if len(neighborhood) > largestNeighborhood:
            largestNeighborhood=len(neighborhood)
    
        l.append(len(neighborhood))
        allNeighbors.extend(neighborhood) #adds the current neighborhood's distances to the list of all neighborhoods
        currentIndex+=1
        compareIndex=0
        totalPoints+=1
        distances.clear()

    full= list(zip(points,l))
    sortedNeighbors= sorted(full, reverse=True, key = lambda x: x[1])
    for x in range(10):
        finalCombined.append(sortedNeighbors[x][0])



    average=len(allNeighbors)/totalPoints
    output1= "The average neighborhood size is: " + str(average) + "\n"
    output2= "The size of the largest neighborhood is: " + str(largestNeighborhood) + "\n"
    output3= "The top 10 points with the biggest neighborhoods are:" + str(finalCombined) + "\n"

    outputFile= open(outputFileName, "w")
    outputFile.write(output1)
    outputFile.write(output2)
    outputFile.write(output3)
    outputFile.close()

if __name__ == "__main__":
    main("numbers1.txt", "summary_1.txt")
    main("numbers2.txt", "summary_2.txt")
    






        
    
