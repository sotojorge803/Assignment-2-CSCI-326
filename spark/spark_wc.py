from pyspark import SparkContext
import sys
import boto3

'''
Authors: Jackson Jacobs and Jorge Soto-Ventura

Computes word count for each different word in text files.
'''

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
    
    
context= SparkContext(appName = "WordCount", master= "local")
inputFileName= sys.argv[1]
outputFileName= sys.argv[2]

data= context.textFile("../datasets/" + inputFileName)                                                    
#Seperates each word
words= data.flatMap(lambda line: line.strip().split(" "))
words= words.map(filterAlnum)
words= words.map(pair)
#print(words.collect())
finalWords= words.reduceByKey(lambda a,b: a+b)
finalWords= finalWords.sortBy(lambda x: -x[1])
finalWords= finalWords.take(50)
finalWords= str(finalWords)

outputFile= open(outputFileName, "w")
outputFile.write(finalWords)
outputFile.close()

'''
s3= boto3.resource("s3")
obj= s3.Object("jacobsj-326", "wordcount_out.txt")
obj.put(Body=finalWords)
'''
