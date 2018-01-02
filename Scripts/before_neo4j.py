from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import json
import tweepy
import csv
import numpy as np


ckey = ''
csecret = ''
atoken = ''
asecret = ''
auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
api = tweepy.API(auth)

try:
	file1 = open('database2.csv','r')
	dict1 = {}  #stores retweeters; key is original user id
	dict2 = {}  #stores other info such as tweet id,text,username and userflrs; key is original user id
	dict3 = {}  #stores retweeter username and retweeter followers; key is the RETWEETER ID
	reader = csv.reader(file1)
	list1 = list(reader)    #2D array representing the entire database
	j = 0
	while(j<len(list1)):
		user = list1[j][2]
		dict1.setdefault(user,[])
		dict2.setdefault(user,[])	#dict2 - tweetid,text,username,userfollowers	
		values = [list1[j][0],list1[j][1],list1[j][3],list1[j][4]] 
		dict1[user].append(list1[j][5])    #adding first retweeter
		dict2[user] = values		#storing tweet and original user info
		retweeter_id = list1[j][5]
		dict3.setdefault(retweeter_id,[])
		dict3[retweeter_id] = [list1[j][6],list1[j][7]]  #dict3 - retweeter name, retweeter followers
		i= j + 1
		if(j>=len(list1) or i>=len(list1)):
			break
		
		while(list1[i][2]==user):
			dict1[user].append(list1[i][5])   #addding further retweeters
			retweeter_id = list1[i][5]
			dict3.setdefault(retweeter_id,[])
			dict3[retweeter_id] = [list1[i][6],list1[i][7]]
			i = i + 1
			if(i>=len(list1)):
				break
		j = i 
		
	file1.close()
	file2 = open('database_neo4j_FINAL2.csv','a')
	writer = csv.writer(file2)
	headers = ['Tweet_id','Tweet_text','user_id','user_name','user_followers','retweeter_id','retweeter_name','retweeter_followers','retweeter_level','first_level_follower']
	writer.writerow(headers)
	

	for key in dict1:   #DIVISION INTO FIRST, SECOND AND THIRD LEVEL FOLLOWERS
		list0 = dict1[key]
		list2 = [] #first level followers
		for i in list0:
			try:			
				x = api.show_friendship(source_id=key,target_id=i)
				if(x[0].followed_by==True): #Target follows source
					list2.append(i)
					list_x = [dict2[key][0],dict2[key][1],key,dict2[key][2],dict2[key][3],i,dict3[i][0],dict3[i][1],1]
					writer.writerow(list_x) #adding first level followers to user database
					
					list0.remove(i)
			except tweepy.TweepError:
				time.sleep(15*60)
				continue
			except StopIteration:
				break
			
				

		list3 = [] #second level followers
		for j in list0:
			for i in list2: #checking remaining elements of original list with first level followers
				try:				
					x = api.show_friendship(source_id=i,target_id=j)
					if(x[0].followed_by==True): #Target follows source
						list3.append(j)
						list_y = [dict2[key][0],dict2[key][1],key,dict2[key][2],dict2[key][3],j,dict3[j][0],dict3[j][1],2,i]
						writer.writerow(list_y) #adding second level followers
						
						list0.remove(j)
				except tweepy.TweepError:
					time.sleep(15*60)
					continue
				except StopIteration:
					break
		
		list4 = [] #third level followers
		for i in list0: #adding third level followers
				for x in list3:    #checking remaining elements of original list with second level followers
					try:					
						x = api.show_friendship(source_id=x,target_id=i)
						if(x[0].followed_by==True):
							list4.append(i)
							list_z = [dict2[key][0],dict2[key][1],key,dict2[key][2],dict2[key][3],i,dict3[i][0],dict3[i][1],3,x]
							writer.writerow(list_z) #adding third level followers to user database
						
							list0.remove(x)
					except tweepy.TweepError:
						time.sleep(15*60)
						continue
					except StopIteration:
						break
		for i in list0:
			list_a = [dict2[key][0],dict2[key][1],key,dict2[key][2],dict2[key][3],i,dict3[i][0],dict3[i][1],0]
			writer.writerow(list_a) #adding other followers to user database
			
	file2.close()

except Exception,e:
	print str(e)
