# Verify input data

#hdfs dfs -ls input/

#output

#input/join2_genchanA.txt
#input/join2_genchanB.txt
#input/join2_genchanC.txt
#input/join2_gennumA.txt
#input/join2_gennumB.txt
#input/join2_gennumC.txt

#Goal
#gennum files contain show names and their viewers
#genchan files contain show names and their channel. 
#We want to find out the total number of viewer across all shows for the channel BAT.


#Read shows files
show_views_file = sc.textFile("input/join2_gennum?.txt")

#check 
show_views_file.take(2)

#output
#[u'Hourly_Sports,21', u'PostModern_Show,38']

#Parse shows files
#Define a function that splits and parses each line of the dataset.
def split_show_views(line):
	key_value = line.split(',')
	show = key_value[0]
	views = key_value[1]
	return (show,views)
#transform input RDD
show_views = show_views_file.map(split_show_views)



#Read channel files
show_channel_file = sc.textFile("input/join2_genchan?.txt")

#Parse channel files
#Write a function to parse each line of the dataset:
def split_show_channel(line):
	key_value = line.split(',')
	show = key_value[0]
	channel = key_value[1]
	return (show, channel)

#use it to parse channel files
show_channel = show_channel_file.map(split_show_channel)

#Join the 2 datasets 
#join the 2 dataset using the show name as the key.
joined_dataset = show_views.join(show_channel)

#Extract channel as key

#You want to find the total viewers by channel, 
#so you need to create an RDD with the channel as key and all the viewer counts, 
#whichever is the show.

def extract_channel_views(show_views_channel):
	channel = show_views_channel[1][1]
	views = int(show_views_channel[1][0])
	return(channel,views)

# apply this function to the joined dataset to create an RDD of channel and views:
channel_views = joined_dataset.map(extract_channel_views)

#Sum across all channels

#Sum all of the viewers for each channel
def sum_views(a, b):
	sum = a + b
	return sum

channel_views.reduceByKey(sum_views).collect()



#output
#[(u'XYZ', 5208016),
#(u'DEF', 8032799),
#(u'CNO', 3941177),
#(u'BAT', 5099141),
#(u'NOX', 2583583),
#(u'CAB', 3940862),
#(u'BOB', 2591062),
#(u'ABC', 1115974),
#(u'MAN', 6566187)]


