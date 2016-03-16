#load datasetA
fileA = sc.textFile("input/join1_FileA.txt")
#make sure the file content is correct
fileA.collect()

def split_fileA(line):
    # split the input line in word and count on the comma
    key_value  = line.split(",")   #split line, into key and value, returns a list
    word     = key_value[0]  #key is first item in list
    count   = int(key_value[1])   ## turn the count to an integer
    return (word, count)

test_line = "able,991"
split_fileA(test_line)


#Run the map transformation to the fileA RDD
fileA_data = fileA.map(split_fileA)

fileA_data.collect()
#Out[]: [(u'able', 991), (u'about', 11), (u'burger', 15), (u'actor', 22)]


#load datasetB
fileB = sc.textFile("input/join1_FileB.txt")
#make sure the file content is correct
fileB.collect()
#Out[29]: 
#[u'Jan-01 able,5',
# u'Feb-02 about,3',
# u'Mar-03 about,8 ',
# u'Apr-04 able,13',
# u'Feb-22 actor,3',
# u'Feb-23 burger,5',
# u'Mar-08 burger,2',
# u'Dec-15 able,100']

def split_fileB(line):
    # split the input line into word, date and count_string
    key_value = line.split(",")
    date_word = key_value[0].split(" ")
    word = date_word[1]
    date = date_word[0]
    count_string = key_value[1]
    return (word, date + " " + count_string) 

test_line = "Feb-22 actor,3"
split_fileB(test_line)

#Run the map transformation to the fileB RDD
fileB_data = fileB.map(split_fileB)

fileB_data.collect()

#Out[]: 
#[(u'able', u'Jan-01 5'),
 #(u'about', u'Feb-02 3'),
 #(u'about', u'Mar-03 8 '),
 #(u'able', u'Apr-04 13'),
 #(u'actor', u'Feb-22 3'),
 #(u'burger', u'Feb-23 5'),
 #(u'burger', u'Mar-08 2'),
 #(u'able', u'Dec-15 100')]


#Run join
fileB_joined_fileA = fileB_data.join(fileA_data)

#verify the result
fileB_joined_fileA.collect()

#The goal is to join the two datasets using the words as keys and print for each word the wordcount for a specific date and then the total output from A.
#Basically for each word in fileB, we would like to print the date and count from fileB but also the total count from fileA.
#Spark implements the join transformation that given a RDD of (K, V) pairs to be joined with another RDD of (K, W) pairs, 
#returns a dataset that contains (K, (V, W)) pairs.

