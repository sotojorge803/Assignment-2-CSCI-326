from pyspark.sql import SparkSession
import sys
import boto3

'''
Authors: Jackson Jacobs and Jorge Soto-Ventura

Computes word count for each different word in text files.
'''

inputFileName= sys.argv[1]
outputFileName= sys.argv[2]

def filterAlnum(word):
    finalWord=""
    for char in word:
        if char.isalnum():
            finalWord= finalWord + char
    return finalWord

def pair(word):
    if word == "":
        return (word, -1)
    return (word,1)
    
session= SparkSession.builder.appName("WordCount").getOrCreate()    
context= session.sparkContext

data= context.textFile(inputFileName)                                                    
#Seperates each word
words= data.flatMap(lambda line: line.strip().split(" "))
words= words.map(filterAlnum)
words= words.map(pair)
#print(words.collect())
finalWords= words.reduceByKey(lambda a,b: a+b)
finalWords= finalWords.sortBy(lambda x: -x[1])
finalWords= finalWords.take(200)
finalWords= str(finalWords)

s3= boto3.resource("s3")
obj= s3.Object("jacobsj-326", outputFileName)
obj.put(Body=finalWords)


'''
outputList= []
for i in range(50):
    outputList.append(finalWords[i])
outputFile= open(outputFileName, "w")
outputFile.write(str(outputList))
outputFile.close()
'''
