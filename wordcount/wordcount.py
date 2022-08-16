import os
import sys

'''
Authors: Jackson Jacobs and Jorge Soto-Ventura

Counts the frequency of words in a text file. Outputs the top 50 most prevalent words.
'''

'''
Checks to see if the inputted character is a number, letter, space, or newline. Returns True if so, False otherwise.
'''

def checkAlnum(char):
    if char.isalnum():
        return True
    elif char== " ":
        return True
    elif char=="\n":
        return True
    else:
        return False

def main():
    inputFile= sys.argv[1]
    stopwordsFileName= sys.argv[3]
    outputFile= sys.argv[2]
    # Gets the current path, in order to work on VScode (By the way, I found a setting that fixes this problem with VScode.)
    directory= os.path.dirname(os.path.abspath(__file__))
    directory= directory.removesuffix("wordcount")
    directory= directory + "datasets/" + inputFile
            
    file= open(directory, "r", encoding="UTF-8") # Open the file
    stopwordsFile= open("../datasets/" + stopwordsFileName, "r", encoding="UTF-8")
    
    allChars=[] 
    allWords=""
    wordsDict={}
    allStopwords=[]

    for line in stopwordsFile:
        allStopwords= line.split(",")

    for line in file:
        for char in line:
            allChars.append(char) # Stores all the characters in a list, seperately.

    allChars=list(filter(checkAlnum, allChars)) # Filters the character list with the checkAlnum function
    allWords="".join(allChars) # After it is filtered, join the words with nothing in between them.
    allWords=allWords.replace("\n", "") # Replace any newlines with nothing
    allWords=allWords.split(" ") # Turn the string into a list by splitting at the spaces.

    # Check every word in the word list
    for word in allWords:
        word= word.lower() # Convert all the words to lowercase
        if word == "":
            continue # If the current word is nothing, we can ignore and skip
        if word in wordsDict:
            wordsDict[word]+=1 # If the word is already in the dictionary, simply add 1 to the key's value
        else:
            wordsDict[word]= 1 #  Else, the word is not already in the dictionary so we need to add it and set it to 1

    for stopword in allStopwords:
        if stopword in wordsDict:
            wordsDict.pop(stopword)
    
    sortedValues= sorted(list(wordsDict.values()), reverse=True) # Sort the dictionary by value
    sortedKeys = {}
    
    for index in range(50):
        value=sortedValues[index] # For the top 50 values, get the value at index 0-49 (the top 50 value indexes in sortedValues)
        for key in wordsDict.keys(): # For every key in the dict
            if wordsDict[key] == value: # If the key's value is equal to the current value, then we can set the key's value in sortedKeys to the current value
                sortedKeys[key]= value

    # Get the out directory
    outDirectory= directory
    outDirectory= outDirectory.removesuffix("datasets/" + inputFile)
    outputFile=open(outDirectory + "wordcount/" + outputFile, "w")

    # Write to the output file, close the files
    for key, value in sortedKeys.items():
          outputFile.write('{0}, {1}\n'.format(key, value))
    file.close()
    outputFile.close()

if __name__ == "__main__":
    main()
