import sys

'''
Authors: Jorge Soto-Ventura and Jackson Jacobs

Counts the wordpairs in a file.
'''

def checkIsAlnum(char):
    if char.isalnum():
        return True
    elif char== " ":
        return True
    elif char== "\n":
        return True
    else:
        return False
    
def main():
    print("running...")
    # Get the names of the files we need.
    inputFileName= sys.argv[1]
    outputFileName= sys.argv[2]
    stopwordsFileName= sys.argv[3]

    # Opening the files we will need, for read.
    directoryInput= "../datasets/" + inputFileName
    directoryStopwords= "../datasets/" + stopwordsFileName
    inputFile= open(directoryInput, "r", encoding="UTF-8")
    stopwordsFile= open(directoryStopwords, "r", encoding="UTF-8")

    # Datasets we will need.
    allChars=[]
    allWords=""
    pairsDictList={}
    allStopwords=[]

    # Retrieve and store the stopwords in a list.
    for line in stopwordsFile:
        allStopwords= line.split(",")

    for line in inputFile:
        for char in line:
            allChars.append(char)

    inputFile.close()
    stopwordsFile.close()
    
    allChars= list(filter(checkIsAlnum, allChars))
    allWords="".join(allChars)
    allWords=allWords.replace("\n", "  ")
    allWords=allWords.split(" ")

    first=True
    nextWordIndex=1
    for word in allWords:
        if nextWordIndex%100000==0:
            print("running..." + str(nextWordIndex))
        if nextWordIndex==len(allWords):
            break
        word= word.lower()
        nextWord=allWords[nextWordIndex].lower()
        
        if word==" " or nextWord== " ":
            nextWordIndex+=1
            continue
        elif word== "":
            nextWordIndex+=1
            continue
        elif nextWord=="":
            nextWordIndex+=1
            continue
        elif word in allStopwords or nextWord in allStopwords:
            nextWordIndex+=1
            continue
        elif first:
            #print("first: ", nextWordIndex)
            currentCombo= word + "/" + nextWord
            pairsDictList[currentCombo] = 1
            nextWordIndex+=1
            first=False
            continue
        else:
            currentCombo= word + "/" + nextWord
            if currentCombo in pairsDictList:
                pairsDictList[currentCombo]+=1
                nextWordIndex+=1
            else:
                pairsDictList[currentCombo]=1
                nextWordIndex+=1

    print("sorting...")            
    sortedValues= sorted(list(pairsDictList.values()), reverse=True)
    sortedKeys={}

    for i in range(30):
        value=sortedValues[i]
        for key in pairsDictList.keys():
            if pairsDictList[key] == value:
                sortedKeys[key]=value
                
    print("done! writing to output...")
    outputFile=open(outputFileName, "w", encoding= "UTF-8")
    for entry in sortedKeys:
        outputFile.write("Pair: " + entry + " Frequency: " + str(sortedKeys[entry]) + "\n")
    outputFile.close()

if __name__ == "__main__":
    main()
    
    

