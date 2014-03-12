import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import re
import json
from threading import Thread, Timer
import time
import pyodbc
import MySQLdb
from datetime import datetime
import sys


# Go to http://dev.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key="AZANhBIru8RxZV49KacZnw"
consumer_secret="beeee8aUCZLEQCIdtlUlbRFRdBT5YBhcwMgim09CEEs"


# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token="310790307-BVK90gA7GgA2OwtZaUxxD4y2ObolLdBdMnMO47c2"
access_token_secret="IwRbKBXHIhimrC7nQrMf9fW2d9pjuAjjqxarXf1iE7EZV"

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)


class PortfolioListener(StreamListener):
    def __init__(self,port,auth,dbtimer):
        self.port = port
        self.auth = auth
        self.dbtimer = dbtimer
        auth.set_access_token(access_token, access_token_secret)
        super(tweepy.StreamListener, self).__init__()
        self.portDict = self.InitializePortDict()
        self.firstTweet = True
        self.newRec = False
    def on_error(self, status):
        print status
    def on_timeout(self):
        return True # Don't kill the stream
    def createSearchString(self):
        portList = ['${0}'.format(i) for i in self.port]
        SearchStr = ",".join(portList)
        return SearchStr
    def InitializePortDict(self):
        portList = ['${0}'.format(i) for i in self.port]
        portfolio = {tic:0 for tic in portList}
        return portfolio
    def getTweetTics(self,data):
        jsonData =json.loads(data)
        tweetText = jsonData['text']
        pattern = re.compile(r'(\$.+?)(?:\s+?|\"|$)')
        tweetTics = re.findall(pattern, tweetText.lower())
        return tweetTics
    def InsertItems(self):
        now = datetime.now()
        now2 = now.strftime("%Y-%d-%m %H:%M:%S")
        z = [(k,v,now2) for k,v in self.portDict.iteritems()]
        return z
    def processToDb(self):
            #print "" + str(self.firstTweet) + 'I am baller'
            Timer(self.dbtimer, self.processToDb).start()
            x = DBConnection()
            if self.firstTweet == False:
                if self.newRec == True:
                    #x.CreateDBTables()
                    itms = self.InsertItems()
                    #print itms
                    x.cur.executemany("""INSERT INTO ciqSocial (GVKEY,Count,Datadate) VALUES (%s,%s,%s)""",itms) ##v
                    x.dbcnxn.commit()
                    print 'db commited'
                    #print ("insert into tweetCount(TwitTic,TweetCount) values(?,?,?)",itms)
                    #self.newRec = False
                    x.dbcnxn.close()
            #print "time"
            #time.sleep(self.dbtimer)
    def on_data(self,data):
        self.newRec = True # Use this Flag to tell the DB process that theres a new record
        tweetTickers = self.getTweetTics(data)
        for key in self.portDict.keys():
            if key in tweetTickers:
                self.portDict[key] +=1
        if self.firstTweet ==True: 
                print 'Starting DB Insert Process. Records will be processed every ' + str(self.dbtimer) +' seconds\n\n'
                target=self.processToDb()
                self.firstTweet = False #Change flag to false so multiple threads aren't started
        #target = self.processToDb()
        #print "Full Stream: " + data + "Tickers in tweet: "+ str(tweetTickers) + "\nCounts: " + str(self.portDict) +"\n\n"
    def listen(self):
        searchPort = self.createSearchString()
        print 'Listening for '+ searchPort + "\n\n"
        stream = Stream(self.auth,self)
        stream.filter(track=[searchPort])
class DBConnection():
    def __init__(self):
        self.server = "127.0.0.1"
        #print "" + self.server
        self.dbname = "TwitterCount"
        self.un = "root"
        self.pw = "root"
        #self.dbstring = 'host="%s";user="%s";passwd="%s",db ="%s" ' %(self.server,self.un,self.pw,self.dbname)
        self.dbcnxn = MySQLdb.connect (host = "localhost", user = "root", passwd = "root", db = "TwitterCount")
        #print self.dbstring
        #self.dbcnxn = MySQLdb.connect(self.dbstring)
        
        self.cur =self.dbcnxn.cursor()
    def CreateDBTables(self):
        self.cur.execute("create table if not exists TwitterCount.ciqSocial (GVKEY varchar(10) character set utf8,Count int(11),Datadate datetime, Primary Key (GVKEY,Datadate));")
        #print "done"
        self.dbcnxn.commit()
if __name__ == '__main__':
    portfolio = ['msft','ibm','t','amzn','mhf','cvx','xom','goog','jpm','ge','appl','bwa','tsla','intc','sne']
    port = PortfolioListener(portfolio, auth, 600) #Create instance for portfolio
    port.listen()
