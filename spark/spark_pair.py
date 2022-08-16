from pyspark import SparkContext
import sys
import boto3

'''
Authors: Jackson Jacobs and Jorge Soto-Ventura
Computes word count for each different word in text files
'''

def filterAlnum(word):
    finalWord=""
    for char in word:
        if char.isalnum() or char== "/":
            finalWord= finalWord + char
    return finalWord

def pair(word):
    return (word,1)

def combine(line):
    outList=[]
    currentLine=line
    for i in range(len(currentLine)):
        if i==len(currentLine)-1:
            break
        currentWord= currentLine[i]
        nextWord= currentLine[i+1]
        combinedWord= currentWord + "/" + nextWord
        outList.append(combinedWord)
    return outList
        
    
context= SparkContext(appName = "WordCount", master= "local")
inputFileName= sys.argv[1]
outputFileName= sys.argv[2]

data= context.textFile("../datasets/" + inputFileName)
words= data.map(lambda line: line.strip().split(" "))
words= words.flatMap(combine)
words= words.map(filterAlnum)
words= words.map(pair)

finalWords= words.reduceByKey(lambda a,b: a+b)
finalWords= finalWords.sortBy(lambda x: -x[1])
finalWords= finalWords.take(50)
finalWords= str(finalWords)

outputFile= open(outputFileName, "w")
outputFile.write(finalWords)
outputFile.close()


