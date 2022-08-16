import sys
import boto3
import random
import statistics

'''

Authors: Jackson Jaocbs and Jorge Soto-Ventura

Finds the median of datasets based on a stream.
'''

def main():
    inputFile= sys.argv[1]
    outputFileName= sys.argv[2]
    samples= int(sys.argv[3])

    #Stuff we will use to store information.                                                                                                                                                              
    num= 115
    l= [None] * int(samples)
    sampleList= [l.copy() for i in range(num)]
    outputList=[None] * num
    numStorage=[]
    totalLines=0

    s3= boto3.resource("s3")
    obj=s3.Object("326-data-bucket", inputFile)
    objDict= obj.get()
    objStream= objDict["Body"]

    it= objStream.iter_lines(chunk_size=2048)

    currentLines=0
    count=0
    for line in it:
        currentLines+=1
        if currentLines % 10000 == 0:
            print("Still running... Line count: ", currentLines)
        numStorage.clear()
        #print("Current line: ", currentLines)
        line=line.decode("utf-8")                                                                                                                                
        numStorage= line.split(",")
        #print(numStorage)
        count+=1
        
        for y in range(samples):
            randum= random.uniform(1,100)
            prob= (1/count) * 100
            if prob > randum:
                for x in range(len(numStorage)):
                    currentNum=float(numStorage[x])
                    sampleList[x][y]= currentNum

    print("Getting medians...")
    for i in range(len(sampleList)):
        current=sampleList[i]
        #print(current)
        current.sort(key= float)
        median=statistics.median(current)
        outputList[i]=median
        
    print("Writing medians...")    
    outputFile=open(outputFileName, "w")
    outputFile.write(str(outputList))
    outputFile.close()
    print("DONE!")

if __name__== "__main__":
    main()
