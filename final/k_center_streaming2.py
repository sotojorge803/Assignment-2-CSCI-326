import boto3
import sys
import math

'''

Authors: Jackson Jacobs and Jorge Soto-Ventura

Streaming implementation of k-center
'''

def main(lower, upper):
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
        line= line.split(",")
        #print(line)
        coord= line[lower:upper+1]
        #print(coord)
        if len(centers) < k and first:
            centers.append(coord)
            continue
        i=1
        currentMax=0
        #print(line)
        if first:
            for center in centers:
                x= float(center[0])
                y= float(center[1])
                z= float(center[2])
                a= float(center[3])
                b= float(center[4])
                for a in range(i, len(centers)):
                    compareCenter= centers[a]
                    compareX= float(compareCenter[0])
                    compareY= float(compareCenter[1])
                    compareZ= float(compareCenter[2])
                    compareA= float(compareCenter[3])
                    compareB= float(compareCenter[4])
                    distance= math.sqrt(((x-compareX)**2) + ((y-compareY)**2) + ((z-compareZ)**2) + ((a-compareA)**2) + ((b-compareB)**2))
                    if distance > currentMax:
                        #print(center, compareCenter)
                        currentMax= distance
                i+=1
            d= currentMax
        first= False
        pointToCenterDis= 0
        x= float(coord[0])
        y= float(coord[1])
        z= float(coord[2])
        a= float(coord[3])
        b= float(coord[4])
        for center in centers:
            cx= float(center[0])
            cy= float(center[1])
            cz= float(center[2])
            ca= float(center[3])
            cb= float(center[4])
            currentDistance= math.sqrt(((x-cx)**2) + ((y-cy)**2) + ((z-cz)**2) + ((a-ca)**2) + ((b-cb)**2))
            if currentDistance < pointToCenterDis or pointToCenterDis == 0:
                pointToCenterDis= currentDistance

        #print(d, 2*d, pointToCenterDis)
        
        if pointToCenterDis > 2*d:
            centers.append(coord)
            if len(centers) > k:
                newD= 2*d
                newCenters= []
                newCenters.append(centers[0])
                centers.remove(centers[0])
                for center in centers:
                    x= float(center[0])
                    y= float(center[1])
                    z= float(center[2])
                    a= float(center[3])
                    b= float(center[4])
                    for newCenter in newCenters:
                        add= True
                        nx= float(newCenter[0])
                        ny= float(newCenter[1])
                        nz= float(newCenter[2])
                        na= float(newCenter[3])
                        nb= float(newCenter[4])
                        distance= math.sqrt(((x-nx)**2) + ((y-ny)**2) + ((z-nz)**2) + ((a-na)**2) + ((b-nb)**2))
                        if distance < newD:
                            add= False
                            break
                    if add:
                        newCenters.append(center)

                centers= newCenters
                d= newD

    print(centers)
    print(len(centers))
    outputFile= open("k_center_streaming2_out.txt", "w")
    outputFile.write(str(centers) + "\nThe number of centers: " + str(len(centers)))
    outputFile.close()
    
if __name__ == "__main__":
    #main(1,5)
    main(11,15)
