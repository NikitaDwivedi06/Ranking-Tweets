from py2neo import Graph, Node, Relationship, authenticate

authenticate("localhost:7474","neo4j","nikita")	
graph = Graph(authenticate)
text_and_ranks = []  #stores a list of tuples, each tuple has a tweet text and its rank
query = '''
MATCH (n:Tweet)-[:TWEETED_BY]->(p:Original_User) RETURN n.text,p.ID
'''
results = graph.run(query)
influence_factor1 = 3
influence_factor2 = 1
influence_factor3 = 2
for r in results:
	list1 = str(r).split("'")
	tweet_text = str(list1[3])
	original_tweeter = list1[7]
	query2 = '''
		MATCH (u:Original_User)-[:FOLLOWED_BY]->(r:Retweeter) WHERE u.ID = {ID} RETURN r.ID,r.followers
                 '''
	results1 = graph.run(query2,ID=original_tweeter)
	level1 = 0 #count of level1 retweeters' followers
	level2 = 0 #count of level 2 retweeters' followers
	level3 = 0 #count of level 3 retweeters' followers
	rank = 0
	level1_retweeter_list = []  #stores first level retweeter ids
	for r in results1:
		level1_retweeter_list.append(str(r).split("'")[3])
		follower_count = str(r).split("'")[7]
		level1 = level1 + int(follower_count)

	for x in level1_retweeter_list:    #that is, for each level1 retweeter
		query3 = '''
			MATCH (r1:Retweeter)-[:FOLLOWED_BY]->(r2:Retweeter) WHERE r1.ID = {ID} RETURN r2.followers
			'''
		results2 = graph.run(query3,ID=x)   #this will store the follower counts of all the second level followers of this level1 retweeter
		for c in results2:
			level2 = level2 + int(str(c).split("'")[3])		
	query3 = '''
		MATCH (t:Tweet)-[:RETWEETED_BY]->(u:Retweeter) WHERE t.text = {text} RETURN u.followers
		'''
	results3 = graph.run(query3,text = tweet_text)
	for q in results3:
		level3 = level3 + int(str(q).split("'")[3])
	rank = level1 * influence_factor1 + level2 * influence_factor2 + level3 * influence_factor3
	text_and_ranks.append((tweet_text,rank))

def getkey(item):
	return item[1]
text_and_ranks = sorted(text_and_ranks,key=getkey,reverse=True)
for x in text_and_ranks:
	print x
	print '\n\n'
	
