import boto3
import sys
from pyspark.sql import SparkSession

'''
Authors: Jackson Jacobs and Jorge Soto-Ventura
Computes some interesting data about some reddit comments
'''

inputFileName= sys.argv[1]

#Initialize the sparksession
session= SparkSession.builder.appName("Comments").getOrCreate()
data = session.read.json(inputFileName)
data_rdd = data.rdd

# Functions that are needed
def findNumComments(r):
    return (r['author'], 1)

def findSubComments(r):
    return (r['subreddit'], 1)

def findAllUserComments(r):
    commentsScore=[r['score'], r['body']]
    return (r['author'], commentsScore)

def findUserBestComment(r1,r2):
    if r1[0] > r2[0]:
        return r1
    else:
        return r2

def findUserWorstComment(r1, r2):
    if r1[1][0] < r2[1][0]:
        return r1
    else:
        return r2

def initializeAverage(r):
    return (r['author'], [r['score'], 1])

def computeTotals(r1, r2):
    score= r1[0] + r2[0]
    total= r1[1] + 1
    return [score, total]

def findAverage(r):
    score=float(r[1][0])
    total=float(r[1][1])
    average=score/total
    return (r[0], average)
    
sampleSize= 1000
# Find the top users by comment number (numberComments)
numberComments= data_rdd.map(findNumComments)
numberComments= numberComments.reduceByKey(lambda a,b: a+b)
numberComments= numberComments.sortBy(lambda x: -x[1])
numberComments= str(numberComments.take(sampleSize))

# Find the top subreddits by comment number (subComments)
subComments= data_rdd.map(findSubComments)
subComments= subComments.reduceByKey(lambda a,b: a+b)
subComments= subComments.sortBy(lambda x: -x[1])
subComments= str(subComments.take(sampleSize))

# Find highest rated comment of each user (bestCommentsByUser)
bestComments= data_rdd.map(findAllUserComments)
bestCommentsByUser= bestComments.reduceByKey(findUserBestComment)
bestCommentsByUser= bestCommentsByUser.sortBy(lambda x: -x[1][0])

# Find the top comments from the list of top comments (topBestUserComments)
topBestUserComments= str(bestCommentsByUser.take(sampleSize))

# Find the worst rated comment of each user (worstCommentsByUser)
worstCommentsByUser= bestCommentsByUser.sortBy(lambda x: x[1][0])

# Find the worst comments from the list of worst comments (topWorstUserComments)
topWorstUserComments= str(worstCommentsByUser.take(sampleSize))

# Find the top users by average karma (topAverage)
average= data_rdd.map(initializeAverage)
average= average.reduceByKey(computeTotals)
average= average.map(findAverage)
average= average.sortBy(lambda x: -x[1])
topAverage= str(average.take(sampleSize))

# Find the worst users by average karma (bottomAverage)
bottomAverage= average.sortBy(lambda x: x[1])
bottomAverage= str(bottomAverage.take(sampleSize))

#Write to files
#outFile1= open("top_user_count.txt", "w")
#outFile1.write(numberComments)
#outFile1.close()

#outFile2= open("top_sub_count.txt", "w")
#outFile2.write(subComments)
#outFile2.close()

#bestCommentsByUser= str(bestCommentsByUser.collect())
#outFile3= open("top_user_com.txt", "w")
#outFile3.write(str(bestCommentsByUser.collect()))
#outFile3.write("\n\n")
#outFile3.write(topBestUserComments)

#outFile3.close()
#worstCommentsByUser= str(worstCommentsByUser.collect())
#outFile4= open("bot_user_com.txt", "w")
#outFile4.write(str(worstCommentsByUser.collect()))
#outFile4.write("\n\n")
#outFile4.write(topWorstUserComments)
#outFile4.close()

#outFile5= open("top_user_score.txt", "w")
#outFile5.write(topAverage)
#outFile5.close()

#outFile6= open("bot_user_score.txt", "w")
#outFile6.write(bottomAverage)
#outFile6.close()

#Code for comments to work in cluster
s3= boto3.resource("s3")
obj1= s3.Object("jacobsj-326","top_user_count.txt")
obj2= s3.Object("jacobsj-326","top_sub_count.txt")
obj3= s3.Object("jacobsj-326","top_user_com.txt")
obj4= s3.Object("jacobsj-326","bot_user_com.txt")
obj5= s3.Object("jacobsj-326","top_user_score.txt")
obj6= s3.Object("jacobsj-326","bot_user_score.txt")
obj1.put(Body=numberComments)
obj2.put(Body=subComments)
obj3.put(Body=topBestUserComments)
obj4.put(Body=topWorstUserComments)
obj5.put(Body=topAverage)
obj6.put(Body=bottomAverage)
