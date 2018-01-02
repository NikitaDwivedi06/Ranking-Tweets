from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import json
import tweepy
import csv
import datetime
import sys

ckey = ''
csecret = ''
atoken = ''
asecret = ''

database_count = 0
dict1 = {}
limit = 400

def removeTrendsWithLowestCounts(dict1):
    sorted_values = sorted(dict1.values())  #sort the trends count. sorted[0] will be the min count
    i = 0
    removed = 0
    l = len(dict1)
    while(removed<l-limit):
        keys = [k for k in dict1 if dict1[k]==sorted_values[i]] 
        j=0
        while(removed<l-limit and dict1):
            del dict1[keys[i]]
            j = j + 1
            removed = removed + 1
    new_list = (",").join(dict1.keys())
    return new_list

def trends_count(list1):
    for l in list1:
        if l in dict1:
            dict1[l] = dict1[l]+1
        else:
            dict1[l]=1


def Check_Dates(dcheck1,dcheck2,year,month,date):
        if(dcheck1[0]==dcheck2[0]==year and dcheck1[1]==dcheck2[1]==month and dcheck1[2]==dcheck2[2]==date):
            return 1
        else:
            return 0

def get_trends():
        auth1 = tweepy.OAuthHandler(ckey, csecret)
        auth1.set_access_token(atoken, asecret)
        api1 = tweepy.API(auth1)
        trends = api1.trends_place(1)
        data = trends[0] 
        # grab the trends
        trends = data['trends']
        # grab the name from each trend
        names=[]
        for trend in trends:
                names.append(trend['name'])
        
        # put all the names together with a ' ' separating them
        trendsName = ','.join(names)
        #print (trendsName)
        
        return trendsName
        #threading.Timer(60, get_trends).start()
        
        


class listener(StreamListener):
        
        def __init__(self,api=None):
                super(listener, self).__init__()
                self.num_tweets = 0

        def on_data(self, data):
                while True:

                        try:
                                                        
                                all_data = json.loads(data)
                                #text =(all_data["text"])
                                #non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)
                                #print(text.translate(non_bmp_map))  #to display emojis properly on windows
                                if('retweeted_status' in all_data):
                                        date1=str(all_data["created_at"].encode('utf-8'))[2:32]    #date of retweet
                                        
                                        date2=str(all_data["retweeted_status"]["created_at"].encode('utf-8'))[2:32] #date of tweet
                                        d1 = datetime.datetime.strptime(date1,'%a %b %d %H:%M:%S +0000 %Y')
                                        d2 = datetime.datetime.strptime(date2,'%a %b %d %H:%M:%S +0000 %Y')
                                        
                                        dcheck1 = (str(d1).split(" "))[0].split("-")
                                        dcheck2 = (str(d2).split(" "))[0].split("-")
                                        year="2017"
                                        month="01"
                                        date="17"
                                        correct_date = Check_Dates(dcheck1,dcheck2,year,month,date)
                              
                                
                                
                                if((not(all_data["in_reply_to_user_id"])) and ('retweeted_status' in all_data) and (correct_date==1)):

                                    
                                    
                                    a1 = retweeter_id = all_data["user"]["id_str"].encode('utf8')
                                    print(a1)
                                    a2 = retweeter_screenname = (all_data["user"]["screen_name"]).encode('utf8')
                                    a3 = retweeter_followers = (all_data["user"]["followers_count"])
                                    a4 = retweeter_friends = (all_data["user"]["friends_count"])
                                    a5 = retweeter_lists = (all_data["user"]["listed_count"])
                                    a6 = retweet_time = (str(d1).split(" "))[1]
                                    b1 = original_tweet_text = (all_data["retweeted_status"]["text"]).encode('utf8')
                                    b2 = original_tweet_id = (all_data["retweeted_status"]["id_str"]).encode('utf8')
                                    b3 = original_user_id = (all_data["retweeted_status"]["user"]["id_str"]).encode('utf8')
                                    b4 = original_user_screen_name = (all_data["retweeted_status"]["user"]["screen_name"]).encode('utf8')
                                    b5 = original_user_followers = (all_data["retweeted_status"]["user"]["followers_count"])
                                    b6 = original_user_friends = (all_data["retweeted_status"]["user"]["friends_count"])
                                    b7 = original_user_lists = (all_data["retweeted_status"]["user"]["listed_count"])
                                    b8 = original_tweet_time = (str(d2).split(" "))[1]
                                    self.num_tweets += 1
                                        

                                        #with open("database_1.csv","a") as file1:
                                        #       file1_writer = csv.writer(file1)
                                        #        file1_writer.writerow                                                           ([original_tweet_id,original_tweet_text,original_user_id,original_user_screen_name,original_user_followers,retweeter_id,retweeter_screenname,retweeter_followers])
                                        #file1.close()
                                        
                                        #print "COUNT IS" + " " + str(self.num_tweets)
                                if(self.num_tweets>3000):
                                        print ('exceeded')
                                        return False
                                else:
                                        return True
                        except IncompleteRead:
                                print("Incomplete Read")
                                pass
                                #time.sleep(5)
                        except:
                            continue
                         
                

        def on_error(self, status_code):
                print (status_code)
                return False

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
api = tweepy.API(auth)

list1=get_trends()
trends_count(list1.split(","))
while(database_count<334):   #for storing 1 million tweets, 3000 at a time
        twitterStream = Stream(auth, listener())        
        twitterStream.filter(languages=["en"],track=[list1],async=True,stall_warnings=True)
        database_count += 1
        updated_trends = get_trends()    #to update the trends after every 15 minutes
        list1 = (str(set(list1+updated_trends)))
        trends_count(list1.split(","))
        if(len(dict1)>limit):    #more than 400 terms in the track parameter
                list1 = removeTrendsWithLowestCounts(dict1)


