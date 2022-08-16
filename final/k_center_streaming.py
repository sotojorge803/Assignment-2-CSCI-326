import boto3
import sys
import math

'''

Authors: Jackson Jacobs and Jorge Soto-Ventura

Streaming implementation of k-center
'''

inputFile= sys.argv[1]
k= int(sys.argv[2])

s3= boto3.resource("s3")
obj=s3.Object("326-data-bucket", inputFile)
objDict= obj.get()
objStream= objDict["Body"]

it= objStream.iter_lines(chunk_size=2048)

centers= []
totalLines=0
first= True
for line in it:
    #print(totalLines)
    totalLines+=1
    if totalLines % 10000 == 0:
        print("Current line: ", totalLines)
    line= line.decode("utf-8")
    coord= line.split(" ")
    if len(centers) < k and first:
        centers.append(coord)
        continue
    i=1
    currentMax=0
    #print(line)
    if first:
        for center in centers:
            x= int(center[0])
            y= int(center[1])
            z= int(center[2])
            for a in range(i, len(centers)):
                compareCenter= centers[a]
                compareX= int(compareCenter[0])
                compareY= int(compareCenter[1])
                compareZ= int(compareCenter[2])
                distance= math.sqrt(((x-compareX)**2) + ((y-compareY)**2) + ((z-compareZ)**2))
                if distance > currentMax:
                    #print(center, compareCenter)
                    currentMax= distance
            i+=1
        d= currentMax
    first= False
    pointToCenterDis= 0
    x= int(coord[0])
    y= int(coord[1])
    z= int(coord[2])
    for center in centers:
        cx= int(center[0])
        cy= int(center[1])
        cz= int(center[2])
        currentDistance= math.sqrt(((x-cx)**2) + ((y-cy)**2) + ((z-cz)**2))
        if currentDistance < pointToCenterDis or pointToCenterDis == 0:
            pointToCenterDis= currentDistance

    #print(pointToCenterDis, 2*d)
    if pointToCenterDis > 2*d:
        centers.append(coord)
        if len(centers) > k:
            newD= 2*d
            newCenters= []
            newCenters.append(centers[0])
            centers.remove(centers[0])
            for center in centers:
                x= int(center[0])
                y= int(center[1])
                z= int(center[2])
                for newCenter in newCenters:
                    add= True
                    nx= int(newCenter[0])
                    ny= int(newCenter[1])
                    nz= int(newCenter[2])
                    distance= math.sqrt(((x-nx)**2) + ((y-ny)**2) + ((z-nz)**2))
                    if distance < newD:
                        add= False
                        break
                if add:
                    newCenters.append(center)
            #print(newCenters)
            #print(centers)
            centers= newCenters.copy()
            d= newD
            #print(centers)

print(centers)
outputFile= open("k_center_streaming_out.txt", "w")
outputFile.write(str(centers))
outputFile.close()
        
    
