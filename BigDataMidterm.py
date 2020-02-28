from pyspark import SparkConf, SparkContext
import string, enchant
conf=SparkConf().setMaster("local").setAppName("Caesar Cypher")
sc=SparkContext(conf=conf)
filename=("","","")

encrypted=sc.textFile("Encrypted-3.txt")
lines=encrypted.map(lambda lines: str(lines))
lines.persist()

table=str.maketrans('','',string.punctuation+string.digits+" "+"")
words=lines.flatMap(lambda strings:strings.split()).map(lambda strings: strings.translate(table)).filter(lambda x:x!="")

chars=lines.flatMap(lambda strings: list(strings)).map(lambda chars: chars.translate(table)).filter(lambda x:x!="")
lines.unpersist()

wordsCount=words.map(lambda words: (words.lower(),1))\
    .reduceByKey(lambda numOfWord1, numOfWord2: numOfWord1+numOfWord2)

charsCount=chars.map(lambda char: (char.lower(), 1)) \
    .reduceByKey(lambda numOfchar1, numOfchar2:numOfchar1+numOfchar2)

topChars=charsCount.takeOrdered(100, key=lambda x: -x[1])
topWords=wordsCount.takeOrdered(100, key=lambda x: -x[1])

mostFreqChar='a'
disFromE=0
freqChar=0
def pickChar(iteration):

    mostFreqChar=topChars[iteration][0]
    if ord(mostFreqChar)>ord('e'):

        disFromE=ord(mostFreqChar)-ord('e')
        return disFromE
    else:
        disFromE=ord('e')-ord(mostFreqChar)

        return disFromE
def caesar(someText, shiftVal):
    normalAlphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    shiftedAlphabet=""
    count=0
    for elem in normalAlphabet:

        tempchr=chr(ord(elem)+shiftVal)

        if ord(tempchr)>90:
          tmpInt=ord(tempchr)-90
          tempchr=chr(64+tmpInt)

        shiftedAlphabet=shiftedAlphabet+tempchr
        count+=1

    table=str.maketrans(normalAlphabet,shiftedAlphabet)
    return someText.translate(table)

dic=enchant.Dict("en_US")
disFromE=pickChar(0)
count1=0
worked=True
for word in topWords:
    if count1>3:
        freqChar+=1
        disFromE=pickChar(freqChar)
        count1=0
        worked=True

    if not dic.check(caesar(word[0].upper(), disFromE)):
        count1+=1

file=open('output3.txt','w')
fin=open("Encrypted-3.txt")

ogText=fin.readlines()
for line in ogText:
    #print(line)
    #print(caesar(line, disFromE))
    file.write(caesar(line, disFromE))
file.write("\n")
fin.close()
file.close()
