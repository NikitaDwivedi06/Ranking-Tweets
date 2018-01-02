from py2neo import Graph, Node, Relationship, authenticate
import csv


authenticate("","","")	
graph = Graph(authenticate)
file1 = open('database_neo4j.csv','r')
reader = csv.reader(file1)
data = list(reader) #stores the entire database as a 2D array
i = 1
length = len(data)
while(i<=(length-2)):
	tweet = Node("Tweet",ID=data[i][0],text=data[i][1])
	user = Node("Original_User",ID=data[i][2],name=data[i][3],followers=data[i][4],tweetid=data[i][0])
	graph.create(tweet | user)
	graph.create(Relationship(tweet, "TWEETED_BY", user))
	j = i
	flag = 0
	while((j<=length) and (data[j][2] == data[i][2])):     #for adding retweeter nodes
		flag = 1			
		retweeter = Node("Retweeter",ID=data[j][5],name=data[j][6],followers=data[j][7],tweetid=data[j][0])
		graph.create(retweeter)
		if((j<=length) and (int(data[j][8]) == 1)):
			graph.create(Relationship(user, "FOLLOWED_BY",retweeter,level=1))
		elif(int(data[j][8]) == 2): 
			first_level_follower = graph.find_one("Retweeter","ID",{data[j][9],"tweetid",data[j][0]})
			graph.create(Relationship(first_level_follower,"FOLLOWED_BY",retweeter,level=2))
		elif(int(data[j][8]) == 3): 
			second_level_follower = graph.find_one("Retweeter","ID",{data[j][9],"tweetid",data[j][0]})
			graph.create(Relationship(second_level_follower,"FOLLOWED_BY",retweeter,level=3))
		elif((j<=length) and (int(data[j][8]) == 0)): 
			graph.create(Relationship(tweet, "RETWEETED_BY", retweeter))
		j = j + 1
			
	if(flag == 1):
		i = j
	elif(flag==0):
		i = i + 1

file1.close()


