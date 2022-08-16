import boto3
import sys

'''
Authors: Jackson Jacobs and Jorge Soto-Ventura

A practice program for streaming with AWS.
'''

def main():

    inputFile= sys.argv[1]
    outputFileName= sys.argv[2]
    #Stuff we will use to store information.
    num= 115
    avgStore= [None] * num
    ourMax= [None] * num
    ourMin= [None] * num
    nonZero= [0] * num
    numStorage=[]
    totalLines=0

    s3= boto3.resource("s3")
    obj=s3.Object("326-data-bucket", inputFile)
    objDict= obj.get()
    objStream= objDict["Body"]

    it= objStream.iter_lines(chunk_size=2048)

    maxLines=0
    currentLines=0
    for line in it:
        line=line.decode("utf-8")
        totalLines+=1 #Increase the total amount of lines in this file
        numStorage= line.split(",")

        currentColumn=0
        for number in numStorage:
            number=float(number)
            #  nonZero
            if number != 0:
                nonZero[currentColumn]+=1
            # Average
            if not avgStore[currentColumn] == None:
                avgStore[currentColumn]+=number
            else:
                avgStore[currentColumn]=number
            # Max
            if ourMax[currentColumn] == None:
                ourMax[currentColumn]= number
            elif number > ourMax[currentColumn]:
                ourMax[currentColumn]= number
            # Min
            if ourMin[currentColumn] == None:
                ourMin[currentColumn]= number
            elif number < ourMin[currentColumn]:
                ourMin[currentColumn]=number

            currentColumn+=1
        
        #Stopping at a certain amount of lines. If currentLines is 0, will not run
        currentLines+=1
        if maxLines==currentLines and currentLines != 0:
            break

    currentColumn=0
    for number in avgStore:
        avgStore[currentColumn]= number/totalLines
        currentColumn+=1
    
    outputFile=open(outputFileName,"w")
    outputFile.write("Total lines: " + str(totalLines) + "\n")
    outputFile.write("Average: " + str(avgStore) + "\n")
    outputFile.write("Max: " + str(ourMax) + "\n")
    outputFile.write("Min: " + str(ourMin) + "\n")
    outputFile.write("Number of non zeroes: " + str(nonZero) + "\n")
    outputFile.close()
    
if __name__ == "__main__":
    main()
