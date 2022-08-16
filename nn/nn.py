import math
import os

'''
Author: Jackson Jacobs & Jorge Soto-Ventura

Gets the cover of each of the data sets.
'''
def main(file, file2, outputFileName):
    # Used to access the correct files
    directory=os.path.dirname(os.path.abspath(__file__))
    directory=directory.removesuffix("nn")
    directory= directory + "/datasets/" + file

    directory2=os.path.dirname(os.path.abspath(__file__))
    directory2=directory2.removesuffix("nn")
    directory2= directory2 + "/datasets/" + file2

    fileName=file # gotta keep track of this name
    file= open(directory, "r")
    file2= open(directory2, "r")

    xList=[]
    yList=[]
    testXList=[]
    testYList=[]

    closestNeighbors=[]
    closestDistance= None # Set to None first so every distance compared is "less than" it

    # Creates two seperate lists for X and Y variables
    for line in file:
        word=line.split()
        x= int(word[0])
        y= int(word[1])
        xList.append(x)
        yList.append(y)
    file.close()

    # Same as above, except for the testnumbers file
    for line in file2:
        word=line.split()
        x= int(word[0])
        y= int(word[1])
        testXList.append(x)
        testYList.append(y)
    file2.close()

    # Zip both of the sets of lists together to form two points lists
    points=list(zip(xList, yList))
    points2=list(zip(testXList, testYList))
    
    # Global Variables used to compare the points against one another
    compareIndex=0
    currentIndex=0

    # For loop that looks at every point in the testNum file and compares it to every point in the numbers(1,2) file
    for current in points2:
        testNumPoint= points2[currentIndex]
        for point in points:
            numPoint= points[compareIndex]
    
            # Gets the distance between the current point in testNum and the point it is being compared to from numbers file
            distance = math.sqrt(((numPoint[0]- testNumPoint[0])**2) + ((numPoint[1] - testNumPoint[1]))**2)
            
            # If closestDistance is None, that means we are on our first point
            if closestDistance== None:
                closestDistance=distance
                closestNeighbors.append(numPoint)

            # If closestDistance is not None, we need to check if the current distance is less than the distance we have stored. If it is, store this number instead
            elif closestDistance > distance:
                closestDistance=distance
                closestNeighbors[currentIndex]= numPoint

            compareIndex+=1 
        currentIndex+=1
        compareIndex=0
        closestDistance=None # Reset the closest distance to None for the next time around

    outDirectory= directory
    outDirectory= outDirectory.removesuffix("datasets/" + fileName)
    outputFile=open(outDirectory + "nn/" + "out_"+ fileName,"w")

    currentIndex=0
    # For each point in the testNums file, write its corresponding closestNeighbor (0th point in testNum=0th point in closestNeighbors)
    for point in points2:
        outputFile.write(str(point) + "->" + str(closestNeighbors[currentIndex]) + "\n")
        currentIndex+=1
    outputFile.close()
    
if __name__ == "__main__":
    main("numbers1.txt", "testNumbers.txt", "out_numbers1.txt")
    main("numbers2.txt", "testNumbers.txt", "out_numbers2.txt")
