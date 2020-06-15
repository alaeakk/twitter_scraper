from async_timeout import timeout
from datetime import datetime
from bs4 import BeautifulSoup
import sys
import socket
import aiohttp
from fake_useragent import UserAgent
import asyncio
import concurrent.futures
import random
from json import loads
from aiohttp_socks import SocksConnector, SocksVer
from json import loads
from user import User
from tweet import Tweet
import requests
from itertools import cycle
from ts_logger import logger
from time import strftime, localtime
import mysql.connector
from videourl import VideoUrl
import datetime
from _datetime import date

file = open('output.txt','w')

list_ip=[]
list_ports=[]


def get_proxies():
    '''
    function to scrape proxies
 
    ''' 
    response = requests.get('https://free-proxy-list.net/')
    soup = BeautifulSoup(response.text, 'lxml')
    table = soup.find('table',id='proxylisttable')
    list_tr = table.find_all('tr')
    list_td = [elem.find_all('td') for elem in list_tr]
    
    list_td = list(filter(None, list_td))
    for elem in list_td:
        if elem[6].text=='yes':
            list_ip.append(elem[0].text)
    for elem in list_td:
        if elem[6].text=='yes':
            list_ports.append(elem[1].text)
    list_proxies = [':'.join(elem) for elem in list(zip(list_ip, list_ports))]
    return list_proxies   
#proxies = get_proxies()

async def Response(session, proxy,url, params=[]):
    '''
    function to get the json response  
     
    @param session: aiohttp client session 
    @param proxy: used proxy to get the response
    @param url: target url
    @param param: url parameters  
    ''' 
        
    #"http://" + config.Proxy_host + ":" + str(config.Proxy_port)
    with timeout(120):
        async with session.get(url, ssl=False, params=params, proxy=proxy) as response:
            return await response.text()
async def Request(url, connector=None, params=[], headers=[]):
    '''
    function to send request to the website
     
   
    @param url: target url
    @param param: url parameters  
    @param headers: included headers to bypass security
    ''' 
    #proxy_pool = cycle(proxies)
    #proxy="http://"+str(next(proxy_pool))
    proxy=None
    logger.info('using proxy: '+str(proxy))
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        return await Response(session,proxy, url, params)
from urllib.parse import quote
from re import findall
l=[]


#params=[('vertical', 'default'), ('src', 'tyah'), ('include_available_features', '1'), ('include_entities', '1'), ('max_position', -1), ('reset_error_state', 'false'), ('f', 'tweets'), ('q', str(query))]

async def getTweetJsonResp(username,pos,retry,timestamp):
    '''
    function to get the json response  from twitter website to scrape tweets
     
    @param username: username of the account
    @param pos: position where to start scraping
    @param retry: number of tries if an error occured
    ''' 
    
    
    if timestamp!=None:
        params=[('vertical', 'default'), ('src', 'unkn'), ('include_available_features', '1'),('replies', '1'), ('include_entities', '1'), ('max_position', str(pos)), ('reset_error_state', 'false'), ('f', 'tweets'), ('q', str(username)+'%20since%3A'+str(timestamp).split(' ')[0]+'%20until%3A'+str(datetime.datetime.now()).split(' ')[0])]
    else:
        params=[('vertical', 'default'), ('src', 'unkn'), ('include_available_features', '1'),('replies', '1'), ('include_entities', '1'), ('max_position', str(pos)), ('reset_error_state', 'false'), ('f', 'tweets'), ('q', 'from:'+str(username))]
    #params=[('vertical', 'default'), ('src', 'tyah'), ('include_available_features', '1'), ('include_entities', '1'), ('max_position', str(pos)), ('reset_error_state', 'false'), ('f', 'tweets'), ('q', str(query))]
    
    headers={"X-Requested-With": "XMLHttpRequest"}
    url = f"https://twitter.com/i/search/timeline/"
    #url = f"https://twitter.com/i/profiles/show/{username}/timeline/tweets?"
    #url=f"https://twitter.com/i/mrkylefield/conversation/1270902697609383937?f=tweets&vertical=default&include_available_features=1&include_entities=1&max_position=-1"
    try:
        #send the request to the website
        r = await Request(url,params=params, headers=headers)
    except aiohttp.client_exceptions.ClientProxyConnectionError as aio:
        logger.exception('ClientProxyConnectionError {} while requesting "{}"'.format(aio, url))
        if retry>0:
            
            retry=retry-1
            return await getTweetJsonResp(username,pos,retry,timestamp) 
    except aiohttp.client_exceptions.ClientPayloadError as aio:
        logger.exception('ClientPayloadError {} while requesting "{}"'.format(aio, url))
        if retry>0:
            
            retry=retry-1
            return await getTweetJsonResp(username,pos,retry,timestamp)
    except aiohttp.client_exceptions.ClientConnectorError as aio:
        logger.exception('ClientConnectorError {} while requesting "{}"'.format(aio, url))
        if retry>0:
            
            retry=retry-1
            return await getTweetJsonResp(username,pos,retry,timestamp)    
         
    except requests.exceptions.HTTPError as e:
        logger.exception('HTTPError {} while requesting "{}"'.format(e, url))
        if retry>0:
            
            retry=retry-1
            return await getTweetJsonResp(username,pos,retry,timestamp)
    except requests.exceptions.ConnectionError as e:
        logger.exception('ConnectionError {} while requesting "{}"'.format(
            e, url))
        if retry>0:
            
            retry=retry-1
            return await getTweetJsonResp(username,pos,retry,timestamp)
    except requests.exceptions.Timeout as e:
        logger.exception('TimeOut {} while requesting "{}"'.format(
            e, url))
        if retry>0:
            
            retry=retry-1
            return await getTweetJsonResp(pos,retry)
    
    return r


async def getHashtagJsonResp(hashtag,pos,retry):
    '''
    function to get the json response  from twitter website to scrape tweets from hashtags
     
    @param username: hashtag string
    @param pos: position where to start scraping
    @param retry: number of tries if an error occured
    ''' 
    #%23hashtag
    query = quote(hashtag)
    #params=[('vertical', 'default'), ('src', 'unkn'), ('include_available_features', '1'), ('include_entities', '1'), ('max_position', str(pos)), ('reset_error_state', 'false'), ('f', 'tweets'), ('q', 'from:'+str(username))]
    #request url parameters
    params=[('vertical', 'default'), ('src', 'tyah'), ('include_available_features', '1'), ('include_entities', '1'), ('max_position', str(pos)), ('reset_error_state', 'false'), ('f', 'tweets'), ('q', str(query))]
    #request headers
    headers={"X-Requested-With": "XMLHttpRequest"}
    url = f"https://twitter.com/i/search/timeline"
    
    try:
        r = await Request(url,params=params, headers=headers)
    except aiohttp.client_exceptions.ClientProxyConnectionError as aio:
        logger.exception('ClientProxyConnectionError {} while requesting "{}"'.format(aio, url))
        if retry>0:
            
            retry=retry-1
            return await getTweetJsonResp(hashtag,pos,retry) 
    except aiohttp.client_exceptions.ClientPayloadError as aio:
        logger.exception('ClientPayloadError {} while requesting "{}"'.format(aio, url))
        if retry>0:
            
            retry=retry-1
            return await getTweetJsonResp(hashtag,pos,retry)
    except aiohttp.client_exceptions.ClientConnectorError as aio:
        logger.exception('ClientConnectorError {} while requesting "{}"'.format(aio, url))
        if retry>0:
            
            retry=retry-1
            return await getTweetJsonResp(hashtag,pos,retry)    
         
    except requests.exceptions.HTTPError as e:
        logger.exception('HTTPError {} while requesting "{}"'.format(e, url))
        if retry>0:
            
            retry=retry-1
            return await getTweetJsonResp(hashtag,pos,retry)
    except requests.exceptions.ConnectionError as e:
        logger.exception('ConnectionError {} while requesting "{}"'.format(
            e, url))
        if retry>0:
            
            retry=retry-1
            return await getTweetJsonResp(hashtag,pos,retry)
    except requests.exceptions.Timeout as e:
        logger.exception('TimeOut {} while requesting "{}"'.format(
            e, url))
        if retry>0:
            
            retry=retry-1
            return await getTweetJsonResp(pos,retry)
    
    return r


import json


async def getConversation(username,conversation,pos,retry):
    '''
    function to get the json response from twitter website to scrape user data
     
    @param username: username of the account
    @param retry: number of tries if an error occured
    ''' 


    params=None
    headers={"X-Requested-With": "XMLHttpRequest"}
    
    url = f"https://twitter.com/elonmusk/status/1270690370968604672"
    url = f'https://api.twitter.com/2/timeline/conversation/1270690370968604672.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&cursor=LBn2ToDAvKmshZujI4jAvJW0v7iiI4DAvtWswOqiI4DAtfnfjuKiI4jAtfG40dyiI4DAvYWtmcmiI4LAvdWTiraiI4qAu%2B3c2OWiI4CAuoHk9rWiI4CAo%2Fm%2FrZ%2BjI4CAppXw77OiI4yAo6XEjZyjI4CAo9nA6r2iI4TAp9Hc9sWiI4CAu4WBgsejI4DAvK2rta2kI4CAsK3r8LeiI4CAu62y2bOiI4SAp72ejb2iI4DAvIGCk%2BiiI4TAvq2eiuKiI4DAtd2Aiu2iI4KAqLHGstyiI4SAvKWMtMqiI4DAoKWP6%2B%2BiI4DAq82Mg%2FmiI4DAo%2F2ulOCiI4DAp8md4bqiI4bAp5GEv8SiI4DApvGU9tmiI4KAvNmg%2BrqiI4CAu%2BXavseiI4qAut2MuriiI4DAoNG1g%2BKiI4DAtLH5m7uiI4qAuo2Vn%2BiiI4jAtKHfneKiI5bAtPX1ouGiI4DAtN2DprOiI4zAo43hnOeiI4DAvqWctLWiI4aAtcGwkNyiI4SAu5Xgy92iI4KAu82Q7cClI4TAo%2FHE3b6iI4CAqMGgl76iI4aAu7mUpuGiI4TAtZG%2F%2FbaiI4bAoJm%2F%2FeWjI4DAp%2BH%2F8eWiI4CAurHAyYOjI4DAq52h8I6jI4LAtdGY9PCiI4CAu6W%2FxN2kI4LAvemsn%2FSiI4LAtNWc87SiI4SApq2uxtyjI4TAvsGtrcClI4aAvPWZvMyjI4CAqK3GuOKiI4KAvNXhu7SiI4CAuqn8gcaiI4DAvY3YorSiI4KAvKmJ6NqiI4KAo7mR1byiI4TAvOnshOiiI4yAsKnJgYijI4DAtfnIofuiI4KAuvm3rNuiI4CAvIW9%2FeGiI4SAu62%2F1p%2BjI4DAp9Xlku2iI4TAvKGX2PSiI4KAsPWAr9WjI4SAu6nh8%2FmiI4jAvfmKz7OiI4CAtdGI7OCiI4KAvPHE3MOiIyUEEQAA&ext=mediaStats%2ChighlightedLabel&include_quote_count=true'
    url= f"https://twitter.com/i/"+str(username)+"/conversation/"+str(conversation)+"?f=tweets&vertical=default&include_available_features=1&include_entities=1&max_position="+str(pos)
    print(url)
    try:
        response = await Request(url,params=params, headers=headers)
    except aiohttp.client_exceptions.ClientProxyConnectionError as aio:
        logger.exception('HTTPError {} while requesting "{}"'.format(aio, url))
        if retry>0:
            
            retry=retry-1
            return await getConversation(username,conversation,pos,retry)      
    except aiohttp.client_exceptions.ClientPayloadError as aio:
        logger.exception('ClientPayloadError {} while requesting "{}"'.format(aio, url))
        if retry>0:
            
            retry=retry-1
            return await getConversation(username,conversation,pos,retry)
    except aiohttp.client_exceptions.ClientConnectorError as aio:
        logger.exception('ClientConnectorError {} while requesting "{}"'.format(aio, url))
        if retry>0:
        
            retry=retry-1
            return await getConversation(username,conversation,pos,retry) 
    except requests.exceptions.HTTPError as e:
        logger.exception('HTTPError {} while requesting "{}"'.format(e, url))
        if retry>0:
        
            retry=retry-1
            return await getConversation(username,conversation,pos,retry)
    except requests.exceptions.ConnectionError as e:
        logger.exception('ConnectionError {} while requesting "{}"'.format(
            e, url))
        if retry>0:
        
            retry=retry-1
            return await getConversation(username,conversation,pos,retry)
    except requests.exceptions.Timeout as e:
        logger.exception('TimeOut {} while requesting "{}"'.format(
            e, url))
        if retry>0:
        
            retry=retry-1
            return await getConversation(username,conversation,pos,retry)
    
    return response
    
    


async def getProfileJsonResp(username,retry):
    '''
    function to get the json response from twitter website to scrape user data
     
    @param username: username of the account
    @param retry: number of tries if an error occured
    ''' 
    params=None
    headers={"X-Requested-With": "XMLHttpRequest"}
    
    url = f"https://twitter.com/"+str(username)+"?lang=en"
    try:
        response = await Request(url,params=params, headers=headers)
    except aiohttp.client_exceptions.ClientProxyConnectionError as aio:
        logger.exception('HTTPError {} while requesting "{}"'.format(aio, url))
        if retry>0:
            
            retry=retry-1
            return await getProfileJsonResp(username,retry)      
    except aiohttp.client_exceptions.ClientPayloadError as aio:
        logger.exception('ClientPayloadError {} while requesting "{}"'.format(aio, url))
        if retry>0:
            
            retry=retry-1
            return await getProfileJsonResp(username,retry)
    except aiohttp.client_exceptions.ClientConnectorError as aio:
        logger.exception('ClientConnectorError {} while requesting "{}"'.format(aio, url))
        if retry>0:
        
            retry=retry-1
            return await getProfileJsonResp(username,retry) 
    except requests.exceptions.HTTPError as e:
        logger.exception('HTTPError {} while requesting "{}"'.format(e, url))
        if retry>0:
        
            retry=retry-1
            return await getProfileJsonResp(username,retry)
    except requests.exceptions.ConnectionError as e:
        logger.exception('ConnectionError {} while requesting "{}"'.format(
            e, url))
        if retry>0:
        
            retry=retry-1
            return await getProfileJsonResp(username,retry)
    except requests.exceptions.Timeout as e:
        logger.exception('TimeOut {} while requesting "{}"'.format(
            e, url))
        if retry>0:
        
            retry=retry-1
            return await getProfileJsonResp(username,retry)
    soup = BeautifulSoup(response, "html.parser")
    user=User(soup)


    return user

async def enterUsernamesDB(list):
    '''
    function to enter only usernames column data on the database 
     
    @param list: list of strings which contains usernames
    ''' 
    try:
        cnx=mysql.connector.connect(user='user', password='',host='localhost',database='twitter')
        cursor = cnx.cursor(buffered=True)
        #cursor.execute("Truncate table users")
    except mysql.connector.Error as err:
        
        print(err)
        return None

    for i in list:
        try:
            cursor.execute("SELECT username, COUNT(*) FROM users WHERE username = %s GROUP BY username",(i,))
            # gets the number of rows affected by the command executed
            row_count = cursor.rowcount
        except mysql.connector.Error as err:
        
            print(err)
            return None
        #check if the username already exists
        if row_count == 0:
            
            add_username = ("INSERT INTO users (username) VALUES (%s) ")
            
            d=(i,)
            
            
            cursor.execute(("SET FOREIGN_KEY_CHECKS=0"))
            cursor.execute(add_username,d ) 
            cnx.commit()
            
async def updateUsersTableData():
    '''
    function to update usernames data on the database 
     
    @param list: list of strings which contains usernames
    ''' 
    try:
        cnx=mysql.connector.connect(user='user', password='',host='localhost',database='twitter',use_unicode=True,charset="utf8")
        cursor = cnx.cursor(buffered=True)
    except mysql.connector.Error as err:
        
        print(err)
        return None

    cursor.execute(("SET FOREIGN_KEY_CHECKS=0"))
    user = ("SELECT username FROM users")
    
                        
    cursor.execute(user)
    a=cursor.fetchall()
    #update all users profile data
    for username in a: 
        
        _user=await getProfileJsonResp(str(username[0]),20)
        #u = ("INSERT INTO user (username,user_id,name,website,join_date,tweets,followingcount,likescount,mediacount,is_private,is_verified,profile_img,background_im) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        u = ("UPDATE users SET uid=%s, name=%s,website=%s,join_date=%s,join_time=%s,tweets=%s,followingcount=%s,followerscount=%s,likescount=%s,mediacount=%s,is_private=%s,is_verified=%s,profile_img=%s,background_img=%s,location=%s,last_updated=%s WHERE  username=%s")
        
        
        d=(str(_user.id),str(_user.name),str(_user.url),str(_user.join_date),str(_user.join_time),str(_user.tweets),str(_user.following),str(_user.followers),str(_user.likes),str(_user.media_count),str(_user.is_private),str(_user.is_verified),str(_user.avatar),str(_user.background_image),str(_user.location),str(datetime.datetime.now()),str(username[0]))
        
        cursor.execute(u,d) 
        cnx.commit()
        
        

def extractHtmlFromJsonResponse(r):
    '''
    function to extract html from the json response received from Username function and return position parameter and tweets to feed tweets function      
     
    @param r: json response 
    ''' 
    #load the json file
    json_response = loads(r)
    #extract html from json file
    html = json_response["items_html"]
    
    soup = BeautifulSoup(html, "html.parser")
    #parse htm to extract tweet text 
    feed = soup.find_all("div", "tweet")
    print(json_response["min_position"])
    #return tweet and the value of the position which will be sent in the next request
    return feed, json_response["min_position"]



async def updateTweetTable(username,num):
    '''
    function to store updated data on the database    
     
    @param username: account username
    @param num: number of tweets requested
    ''' 
    logger.info('Scraping tweets from : '+username+' account')
    i=-1
    #counter of the number of tweets (if p>num we break)
    p=0
    #counter to log  the number of tweets scraped 
    counter_log=0
    #counter to log  the number of tweets updated 
    bc=0
    try:
        cnx=mysql.connector.connect(user='user', password='',host='localhost',database='twitter',use_unicode=True,charset="utf8")
        cursor = cnx.cursor(buffered=True)
    except mysql.connector.Error as err:
        
        print(err)
        return None

    
    while p<num:
        #get the json response from the website 
        a=await getTweetJsonResp(username,i,20,None) 
        #get tweets from html extracted from json response
        l,i=extractHtmlFromJsonResponse(a)
        print(str(p)+"fffffffffffffff")
        if len(l)>0:
            if p==0:
                t1=Tweet(l[0])
                timestamp=str(t1.datestamp)+' '+str(t1.timestamp)
                await updateUserTimeStampDB(t1.username, timestamp) 
            for j in l:
                #create a Tweet object 
                print('resfdv')
                t=Tweet(j)
                
                cursor.execute("SELECT tweet_uid, COUNT(*) FROM tweets WHERE tweet_uid = %s GROUP BY tweet_uid",(t.id,))
                row_count = cursor.rowcount
                logger.info('Got '+ str(p)+' tweets from : '+username+' account')
                #check if the Tweet already exists in the Database
                if row_count == 0:
                    counter_log=counter_log+1
                    cursor.execute(("SET FOREIGN_KEY_CHECKS=0"))
                    #select user with the appropriate username
                    tweet1 = ("SELECT id FROM users WHERE username=%s")
                    b=(username,)
                    
                    cursor.execute(tweet1,b)
                    a=cursor.fetchall()
                    print(a)
                    """
                    if t.video==True:
                        #extract the video url from the website
                        video_url=VideoUrl(str(t.link)).geturl()
                        print(video_url)
        -            else:
                        video_url=str(t.video)
                    """ 
                    video_url=str(t.video)
                    await updateConversationIdTableDB (t.conversation_id,t.username)  
                    await updateMentionsTableDB(t.mentions) 
                    await updateHashtagTableDB(t.hashtags)   
                    #insert tweets in DB              
                    tweet = ("INSERT INTO tweets (user_uid,tweet_uid,datetime,timestamp,user_id,username,name,place,text,repliescount,retweetscount,likescount,link,text_html,video_links,imagelinks,links,retweet,reply_to,hashtags,cashtags,user_rt_id,retweet_id,quote_url,mentions,conversation_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
                    d=(str(t.user_id),int(t.id),str(t.datestamp),str(t.timestamp),int(a[0][0]),str(t.username),str(t.name),str(t.place),str(t.text),int(t.replies_count),int(t.retweets_count),int(t.likes_count),str(t.link),str(t.text_html),video_url,str(t.photos),str(t.urls),str(t.retweet),str(t.reply_to),str(t.hashtags),str(t.cashtags),str(t.user_rt_id),str(t.user_rt),str(t.quote_url),str(t.mentions),str(t.conversation_id))
                    
                    logger.info(str(counter_log)+' tweets stored in database')    
                

                
                    cursor.execute(tweet,d) 
                
                    
                    cnx.commit()
                else:
                    bc=bc+1
                    cursor.execute(("SET FOREIGN_KEY_CHECKS=0"))
                    #select user with the appropriate username
                    tweet1 = ("SELECT id FROM users WHERE username=%s")
                    b=(username,)
                    
                    cursor.execute(tweet1,b)
                    a=cursor.fetchall()
                    """
                    if t.video==True:
                        #extract the video url from the website
                        video_url=VideoUrl(str(t.link)).geturl()
                        print(video_url)
                    else:
                        video_url=str(t.video)
                    """ 
                    video_url=str(t.video)
                    
                    #update tweets data in DB
                    tweet = ("UPDATE tweets SET user_uid=%s,datetime=%s,timestamp=%s,user_id=%s,username=%s,name=%s,place=%s,text=%s,repliescount=%s,retweetscount=%s,likescount=%s,link=%s,text_html=%s,video_links=%s,imagelinks=%s,links=%s,retweet=%s,reply_to=%s,hashtags=%s,cashtags=%s,user_rt_id=%s,retweet_id=%s,quote_url=%s,mentions=%s,conversation_id=%s WHERE tweet_uid=%s")
                    d=(str(t.user_id),str(t.datestamp),str(t.timestamp),int(a[0][0]),str(t.username),str(t.name),str(t.place),str(t.text),int(t.replies_count),int(t.retweets_count),int(t.likes_count),str(t.link),str(t.text_html),video_url,str(t.photos),str(t.urls),str(t.retweet),str(t.reply_to),str(t.hashtags),str(t.cashtags),str(t.user_rt_id),str(t.user_rt),str(t.quote_url),str(t.mentions),str(t.conversation_id),str(t.id))
                    
                    logger.info(str(bc)+' tweets updated in database')    
                

                
                    cursor.execute(tweet,d) 
                
                    
                    cnx.commit()

                p=p+1
                
                if p>=num:
    
                    break
        p=p+1
    #await updateUserTimeStampDB(username)
    
async def updateMentionsTableDB(list):
    '''
    function to update mentions table in DB     
     
    @param list: list of mentions
    
    ''' 
    try:
        cnx=mysql.connector.connect(user='user', password='',host='localhost',database='twitter',use_unicode=True,charset="utf8")
        cursor = cnx.cursor(buffered=True)
    except mysql.connector.Error as err:
        
        print(err)
        return None
    if len(list)>0:
        for mention in list:
            
            cursor.execute("SELECT mention, COUNT(*) FROM mentions WHERE mention = %s GROUP BY mention",(mention,))
            row_count = cursor.rowcount
            #check if the mention already exists
            if row_count == 0:
                tweet = ("INSERT INTO mentions (mention,date) VALUES (%s,%s)")
                d=(str(mention),str(datetime.datetime.now()))
                cursor.execute(tweet,d) 
                cnx.commit()
            else:
                
                cursor.execute("UPDATE mentions SET count = count + 1 WHERE mention=%s",(str(mention ),))
                cnx.commit()

async def updateConversationIdTableDB(conversation,username):
    '''
    function to  update conversationid table in DB    
     
    @param conversation: id of the conversation
    
    ''' 
    try:
        cnx=mysql.connector.connect(user='user', password='',host='localhost',database='twitter',use_unicode=True,charset="utf8")
        cursor = cnx.cursor(buffered=True)
    except mysql.connector.Error as err:
        
        print(err)
        return None


    cursor.execute("SELECT conversationid, COUNT(*) FROM conversation_id WHERE conversationid = %s GROUP BY conversationid",(conversation,))
    row_count = cursor.rowcount
    #check if the conversationid already exists
    if row_count == 0:
        tweet = ("INSERT INTO conversation_id (conversationid,date,username) VALUES (%s,%s,%s)")
        d=(int(conversation),str(datetime.datetime.now()),str(username))
        cursor.execute(tweet,d) 
        cnx.commit()
    else:
        cursor.execute("UPDATE conversation_id SET count = count + 1 WHERE conversationid=%s",(int(conversation ),))
        cnx.commit()

        

async def updateHashtagTableDB(list):
    '''
    function to  update hashtags table in DB     
     
    @param list: list of hashtags
    
    ''' 
    try:
        cnx=mysql.connector.connect(user='user', password='',host='localhost',database='twitter',use_unicode=True,charset="utf8")
        cursor = cnx.cursor(buffered=True)
    except mysql.connector.Error as err:
        
        print(err)
        return None


    
    if len(list)>0:    
        for hashtag in list:
            cursor.execute("SELECT hashtag, COUNT(*) FROM hashtags WHERE hashtag = %s GROUP BY hashtag",(hashtag,))
            row_count = cursor.rowcount
            #check if the hashtag already exists
            if row_count == 0:
                tweet = ("INSERT INTO hashtags (hashtag,date) VALUES (%s,%s)")
                
                d=(str(hashtag),str(datetime.datetime.now()))
                cursor.execute(tweet,d) 
                cnx.commit()
            else:
                
                #increment hashtags count 
                cursor.execute("UPDATE hashtags SET count = count + 1 WHERE hashtag=%s",(str(hashtag ),))
                cnx.commit()

async def processMentions():
    """
    function to extract usernames from mentions extracted from tweets and update users table with new usernames
    """    
    try:
        cnx=mysql.connector.connect(user='user', password='',host='localhost',database='twitter',use_unicode=True,charset="utf8")
        cursor = cnx.cursor(buffered=True)
    except mysql.connector.Error as err:
        
        print(err)
        return None
    l=[]
    cursor.execute("SELECT mention FROM mentions WHERE processed=%s",(str(0),))
    #get unprocessed mentions from DB
    a=cursor.fetchall()
   
    
    #update mentions table in DB
    cursor.execute("UPDATE mentions SET processed =%s WHERE processed =%s",(str(1),str(0)))
    cnx.commit()
    for i in a:
        l.append(str(i[0]))
    await enterUsernamesDB(l)    
    cnx.commit()    
    for i in a:
        
        await updateTweetTable(str(i[0]),20)
    #update users table in DB
    
    await updateUsersTableData()
    
    
async def processHashtags():
    """
    function to extract usernames from hashtag tweets and update users table with new usernames
    """
    try:
        cnx=mysql.connector.connect(user='user', password='',host='localhost',database='twitter',use_unicode=True,charset="utf8")
        cursor = cnx.cursor(buffered=True)
    except mysql.connector.Error as err:
        
        print(err)
        return None
    #get unprocessed hashtags from DB
    cursor.execute("SELECT hashtag FROM hashtags WHERE processed=%s",(str(0),))
    a=cursor.fetchall()
    


    
    if len(a)>0:    
        for h in a[0]: 
            counter_log=0
            i=-1 
            first_tweet=1  
            while True:
                
                #get the json response from the website 
                a=await getHashtagJsonResp(h,i,20) 
                #get tweets from html extracted from json response
                l,i=extractHtmlFromJsonResponse(a)
                l1=[]
                if len(l)>0:
                    
                    for r in l:
                        
                        #create a Tweet object 
                        t=Tweet(r)
                        
                        
                        l1.append(t.username)
                        print(l1)
                    #update users table in DB with new usernames
                    await enterUsernamesDB(l1)
                    cnx.commit()

                    if first_tweet==1:
                        t1=Tweet(l[0])
                        timestamp=str(t1.datestamp)+' '+str(t1.timestamp)
                        await updateUserTimeStampDB(t1.username, timestamp)
                        first_tweet=0                    
                    for j in l:
                        
                        #create a Tweet object 
                        t=Tweet(j)
                        
                        #check if the Tweet already exists in the Database
                        cursor.execute("SELECT tweet_uid, COUNT(*) FROM tweets WHERE tweet_uid = %s GROUP BY tweet_uid",(t.id,))
                        row_count = cursor.rowcount
                        
                        
                        
                        
                        if row_count == 0:
                            
                            counter_log=counter_log+1
                            cursor.execute(("SET FOREIGN_KEY_CHECKS=0"))
                            #select user with the appropriate username
                            tweet1 = ("SELECT id FROM users WHERE username=%s")
                            b=(str(t.username),)
                            print(b)
                            cursor.execute(tweet1,b)
                            a=cursor.fetchall()
                            print(a)
                            """
                            if t.video==True:
                                #extract the video url from the website
                                video_url=VideoUrl(str(t.link)).geturl()
                                print(video_url)
                            else:
                                video_url=str(t.video)
                            """ 
                            video_url=str(t.video)
                            await updateConversationIdTableDB (t.conversation_id,t.username)  
                            await updateMentionsTableDB(t.mentions) 
                            await updateHashtagTableDB(t.hashtags)  
                            #insert new tweets in tweets table in DB
                            tweet = ("INSERT INTO tweets (user_uid,tweet_uid,datetime,timestamp,user_id,username,name,place,text,repliescount,retweetscount,likescount,link,text_html,video_links,imagelinks,links,retweet,reply_to,hashtags,cashtags,user_rt_id,retweet_id,quote_url,mentions,conversation_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
                            if len(a)>0:
                                d=(str(t.user_id),int(t.id),str(t.datestamp),str(t.timestamp),int(a[0][0]),str(t.username),str(t.name),str(t.place),str(t.text),int(t.replies_count),int(t.retweets_count),int(t.likes_count),str(t.link),str(t.text_html),video_url,str(t.photos),str(t.urls),str(t.retweet),str(t.reply_to),str(t.hashtags),str(t.cashtags),str(t.user_rt_id),str(t.user_rt),str(t.quote_url),str(t.mentions),str(t.conversation_id))
                            
                                logger.info(str(counter_log)+' tweets stored in database')    
                        
        
                        
                                cursor.execute(tweet,d) 
                        
                            
                                cnx.commit()
                
        await updateUsersTableData()    
        #await updateUserTimeStampDB(t.username)    
        cursor.execute("UPDATE hashtags SET processed =%s WHERE processed =%s",(str(1),str(0)))
        cnx.commit()

async def getConversationUsername(username,id,retry):
    '''
    function to get the json response from twitter website to scrape user data
     
    @param username: username of the account
    @param retry: number of tries if an error occured
    ''' 
    params=None
    headers={"X-Requested-With": "XMLHttpRequest"}
    
    url = f"https://twitter.com/"+str(username)+"/status/"+str(id)+"?lang=en"
    try:
        response = await Request(url,params=params, headers=headers)
    except aiohttp.client_exceptions.ClientProxyConnectionError as aio:
        logger.exception('HTTPError {} while requesting "{}"'.format(aio, url))
        if retry>0:
            
            retry=retry-1
            return await getConversationUsername(username,id,retry)      
    except aiohttp.client_exceptions.ClientPayloadError as aio:
        logger.exception('ClientPayloadError {} while requesting "{}"'.format(aio, url))
        if retry>0:
            
            retry=retry-1
            return await getConversationUsername(username,id,retry)
    except aiohttp.client_exceptions.ClientConnectorError as aio:
        logger.exception('ClientConnectorError {} while requesting "{}"'.format(aio, url))
        if retry>0:
        
            retry=retry-1
            return await getConversationUsername(username,id,retry) 
    except requests.exceptions.HTTPError as e:
        logger.exception('HTTPError {} while requesting "{}"'.format(e, url))
        if retry>0:
        
            retry=retry-1
            return await getConversationUsername(username,id,retry)
    except requests.exceptions.ConnectionError as e:
        logger.exception('ConnectionError {} while requesting "{}"'.format(
            e, url))
        if retry>0:
        
            retry=retry-1
            return await getConversationUsername(username,id,retry)
    except requests.exceptions.Timeout as e:
        logger.exception('TimeOut {} while requesting "{}"'.format(
            e, url))
        if retry>0:
        
            retry=retry-1
            return await getConversationUsername(username,id,retry)
    soup = BeautifulSoup(response, "html.parser")

    
    
    print(str(soup).encode(encoding='utf_8', errors='strict'))
    username=str(soup.find_all('div',{"class": "permalink-inner"})[0].find('div',{"class": "tweet"})['data-screen-name'])
    


    return username
   
    


async def processConversation():
    """
    function to extract usernames from hashtag tweets and update users table with new usernames
    """
    try:
        cnx=mysql.connector.connect(user='user', password='',host='localhost',database='twitter',use_unicode=True,charset="utf8")
        cursor = cnx.cursor(buffered=True)
    except mysql.connector.Error as err:
        
        print(err)
        return None
    #get unprocessed hashtags from DB
    cursor.execute("SELECT conversationid,username FROM conversation_id WHERE processed=%s",(str(0),))
    a=cursor.fetchall()
    


    
    if len(a)>0:    
        for h in a: 
            counter_log=0
            i=-1  
            first_tweet=1
            while True:
                
                
                #get the json response from the website 
                username=await getConversationUsername(str(h[1]),int(h[0]),20)
                print(username)
                a=await getConversation(username,int(h[0]),i,20) 
                #get tweets from html extracted from json response
                l,i=extractHtmlFromJsonResponse(a)
                l1=[]
                if len(l)>0:
                    
                    for r in l:
                        
                        #create a Tweet object 
                        t=Tweet(r)
                        
                        
                        l1.append(t.username)
                        print(l1)
                    #update users table in DB with new usernames
                    await enterUsernamesDB(l1)
                    cnx.commit()
                    if first_tweet==1:
                        t1=Tweet(l[0])
                        timestamp=str(t1.datestamp)+' '+str(t1.timestamp)
                        await updateUserTimeStampDB(t1.username, timestamp)
                        first_tweet=0
                    for j in l:
                        
                        #create a Tweet object 
                        t=Tweet(j)
                        
                        #check if the Tweet already exists in the Database
                        cursor.execute("SELECT tweet_uid, COUNT(*) FROM tweets WHERE tweet_uid = %s GROUP BY tweet_uid",(t.id,))
                        row_count = cursor.rowcount
                        
                        
                        
                        
                        if row_count == 0:
                            
                            counter_log=counter_log+1
                            cursor.execute(("SET FOREIGN_KEY_CHECKS=0"))
                            #select user with the appropriate username
                            tweet1 = ("SELECT id FROM users WHERE username=%s")
                            b=(str(t.username),)
                            print(b)
                            cursor.execute(tweet1,b)
                            a=cursor.fetchall()
                            print(a)
                            """
                            if t.video==True:
                                #extract the video url from the website
                                video_url=VideoUrl(str(t.link)).geturl()
                                print(video_url)
                            else:
                                video_url=str(t.video)
                            """ 
                            video_url=str(t.video)
                            await updateConversationIdTableDB (t.conversation_id,t.username)  
                            await updateMentionsTableDB(t.mentions) 
                            await updateHashtagTableDB(t.hashtags)  
                            #insert new tweets in tweets table in DB
                            tweet = ("INSERT INTO tweets (user_uid,tweet_uid,datetime,timestamp,user_id,username,name,place,text,repliescount,retweetscount,likescount,link,text_html,video_links,imagelinks,links,retweet,reply_to,hashtags,cashtags,user_rt_id,retweet_id,quote_url,mentions,conversation_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
                            if len(a)>0:
                                d=(str(t.user_id),int(t.id),str(t.datestamp),str(t.timestamp),int(a[0][0]),str(t.username),str(t.name),str(t.place),str(t.text),int(t.replies_count),int(t.retweets_count),int(t.likes_count),str(t.link),str(t.text_html),video_url,str(t.photos),str(t.urls),str(t.retweet),str(t.reply_to),str(t.hashtags),str(t.cashtags),str(t.user_rt_id),str(t.user_rt),str(t.quote_url),str(t.mentions),str(t.conversation_id))
                            
                                logger.info(str(counter_log)+' tweets stored in database')    
                        
        
                        
                                cursor.execute(tweet,d) 
                        
                            
                                cnx.commit()
                
        await updateUsersTableData()    
        #await updateUserTimeStampDB(t.username)    
        cursor.execute("UPDATE conversation_id SET processed =%s WHERE processed =%s",(str(1),str(0)))
        cnx.commit()














async def updateUserTimeStampDB(username,timestamp):
    '''
    function to  insert the new timestamp column in the users table after an update    
     
    @param username: account username
    
    ''' 
    try:
        cnx=mysql.connector.connect(user='user', password='',host='localhost',database='twitter',use_unicode=True,charset="utf8")
        cursor = cnx.cursor(buffered=True)
    except mysql.connector.Error as err:
        
        print(err)
        return None
    cursor.execute(("SET FOREIGN_KEY_CHECKS=0"))
    
    #update last_tweet_timestamp in users table  
    u = ("UPDATE users SET last_tweet_timestamp =%s WHERE username=%s" )
    d=(str(timestamp),username)

    cursor.execute(u,d) 
    cnx.commit()
    
async def getTweets(username):

    '''
    function to scrape new tweets after the last update and then update tweets table in DB  
     
    @param username: account username
    ''' 
    logger.info('Scraping tweets from : '+username+' account')
    i=-1
    p=0
    counter_log=0
    bc=0
    try:
        cnx=mysql.connector.connect(user='user', password='',host='localhost',database='twitter',use_unicode=True,charset="utf8")
        cursor = cnx.cursor(buffered=True)
    except mysql.connector.Error as err:
        
        print(err)
        return None
    
    cursor.execute(("SET FOREIGN_KEY_CHECKS=0"))
    #select user with the appropriate username
    tweet1 = ("SELECT last_tweet_timestamp  FROM users WHERE username=%s")
    b=(username,)
    
    cursor.execute(tweet1,b)
    _timestamp=cursor.fetchall()
    #check the last tweet time stamp 
    if (_timestamp[0][0]!=None):
        while True:
            
            a=await getTweetJsonResp(username,i,20,_timestamp[0][0]) 
            #get new tweets after the last update 
            l,i=extractHtmlFromJsonResponse(a)
            
            _timestamp=datetime.datetime(int(_timestamp[0][0].split(' ')[1].split('-')[0]),int(_timestamp[0][0].split(' ')[1].split('-')[1]),int(_timestamp[0][0].split(' ')[1].split('-')[2]),int(_timestamp[0][0].split(' ')[0].split(':')[0]),int(_timestamp[0][0].split(' ')[0].split(':')[1]),int(_timestamp[0][0].split(' ')[0].split(':')[2]))
            
            if len(l)>0:
                for j in l:
                    #create a Tweet object 
                    
                    t=Tweet(j)
                    

                    

                        
                    counter_log=counter_log+1
                    cursor.execute(("SET FOREIGN_KEY_CHECKS=0"))
                    #select user with the appropriate username
                    tweet1 = ("SELECT id FROM users WHERE username=%s")
                    b=(username,)
                    
                    cursor.execute(tweet1,b)
                    a=cursor.fetchall()
                    """
                    if t.video==True:
                        #extract the video url from the website
                        video_url=VideoUrl(str(t.link)).geturl()
                        print(video_url)
                    else:
                        video_url=str(t.video)
                    """ 
                    video_url=str(t.video)
                    tweet = ("INSERT INTO tweets (user_uid,tweet_uid,datetime,timestamp,user_id,username,name,place,text,repliescount,retweetscount,likescount,link,text_html,video_links,imagelinks,links,retweet,reply_to,hashtags,cashtags,user_rt_id,retweet_id,quote_url,mentions,conversation_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
                    d=(str(t.user_id),int(t.id),str(t.datestamp),str(t.timestamp),int(a[0][0]),str(t.username),str(t.name),str(t.place),str(t.text),int(t.replies_count),int(t.retweets_count),int(t.likes_count),str(t.link),str(t.text_html),video_url,str(t.photos),str(t.urls),str(t.reply_to),str(t.hashtags),str(t.cashtags),str(t.user_rt_id),str(t.user_rt),str(t.quote_url),str(t.mentions),str(t.conversation_id))
                    
                    logger.info(str(counter_log)+' tweets stored in database')    
                
    
                
                    cursor.execute(tweet,d) 
                
                    
                    cnx.commit()
        await updateTweetTable(username,20)
                  
    else:
        await updateTweetTable(username,20)
        
                   
def fillUsernameDb(list):
    '''
    calling the coroutine enterUsernamesDB()
    :param list: list of usernames 
    :type list:
    '''
    loop = asyncio.get_event_loop()
    loop.run_until_complete(enterUsernamesDB(list))
def run(username,num):    
    '''
    calling the coroutine updateTweetTable()
    :param username:account username
    :type num:number of tweets
    :param num:number of tweets requested
    '''
    loop = asyncio.get_event_loop()
    loop.run_until_complete(updateTweetTable(username,num))
def update_Users_TableData():
    '''
    calling the coroutine enterOtherUserData()

    '''
    loop = asyncio.get_event_loop()
    loop.run_until_complete(updateUsersTableData())
def get_Tweets(username):
    '''
    calling the coroutine enterOtherUserData()
    :param username:username of the account
    :type list:
    '''
    loop = asyncio.get_event_loop()
    loop.run_until_complete(getTweets(username))   
def process_Mentions():
    '''
    calling the coroutine processMentions()

    '''
    loop = asyncio.get_event_loop()
    loop.run_until_complete(processMentions())
def process_Hashtags():
    '''
    calling the coroutine processHashtags()

    '''
    loop = asyncio.get_event_loop()
    loop.run_until_complete(processHashtags())   
def process_Conversation():
    '''
    calling the coroutine processHashtags()

    '''
    loop = asyncio.get_event_loop()
    loop.run_until_complete(processConversation())  