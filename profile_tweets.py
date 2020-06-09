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

file = open('output.txt','w')

list_ip=[]
list_ports=[]
def get_proxies():
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
    
    
    
    #"http://" + config.Proxy_host + ":" + str(config.Proxy_port)
    
    with timeout(120):
        async with session.get(url, ssl=False, params=params, proxy=proxy) as response:
            return await response.text()
async def Request(url, connector=None, params=[], headers=[]):
    proxy=None
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        return await Response(session,proxy, url, params)
from urllib.parse import quote
from re import findall
l=[]


#params=[('vertical', 'default'), ('src', 'tyah'), ('include_available_features', '1'), ('include_entities', '1'), ('max_position', -1), ('reset_error_state', 'false'), ('f', 'tweets'), ('q', str(query))]

async def Username(username,pos,retry):
    query = quote(username)
    params=[('vertical', 'default'), ('src', 'unkn'), ('include_available_features', '1'), ('include_entities', '1'), ('max_position', str(pos)), ('reset_error_state', 'false'), ('f', 'tweets'), ('q', 'from:'+str(username))]
    #params=[('vertical', 'default'), ('src', 'tyah'), ('include_available_features', '1'), ('include_entities', '1'), ('max_position', str(pos)), ('reset_error_state', 'false'), ('f', 'tweets'), ('q', str(query))]
    headers={"X-Requested-With": "XMLHttpRequest"}
    url = f"https://twitter.com/i/search/timeline"
    
    try:
        r = await Request(url,params=params, headers=headers)
    except aiohttp.client_exceptions.ClientProxyConnectionError as aio:
        logger.exception('HTTPError {} while requesting "{}"'.format(aio, url))
        if retry>0:
            print("retry")
            retry=retry-1
            return await Username(username,pos,retry)        
    except requests.exceptions.HTTPError as e:
        logger.exception('HTTPError {} while requesting "{}"'.format(e, url))
        if retry>0:
            print("retry")
            retry=retry-1
            return await Username(username,pos,retry)
    except requests.exceptions.ConnectionError as e:
        logger.exception('ConnectionError {} while requesting "{}"'.format(
            e, url))
        if retry>0:
            print("retry")
            retry=retry-1
            return await Username(username,pos,retry)
    except requests.exceptions.Timeout as e:
        logger.exception('TimeOut {} while requesting "{}"'.format(
            e, url))
        if retry>0:
            print("retry")
            retry=retry-1
            return await Username(pos,retry)
    
    return r




def inf(ur, _type):
 
    
    try:
        group = ur.find("div", "user-actions btn-group not-following ")
        if group == None:
            group = ur.find("div", "user-actions btn-group not-following")
        if group == None:
            group = ur.find("div", "user-actions btn-group not-following protected")
    except Exception as e:
        print("Error: " + str(e))

    if _type == "id":
        ret = group["data-user-id"]
    elif _type == "name":
        ret = group["data-name"]
    elif _type == "username":
        ret = group["data-screen-name"]
    elif _type == "private":
        ret = group["data-protected"]
        if ret == 'true':
            ret = 1
        else:
            ret = 0

    return ret
def stat(ur, _type):
    
    _class = f"ProfileNav-item ProfileNav-item--{_type}"
    stat = ur.find("li", _class)
    try :
        return int(stat.find("span", "ProfileNav-value")["data-count"])
    except AttributeError:
        return 0

async def query_user_page(username):

    params=None
    headers={"X-Requested-With": "XMLHttpRequest"}
    
    url = f"https://twitter.com/"+str(username)+"?lang=en"
   
    response = await Request(url,params=params, headers=headers)
    soup = BeautifulSoup(response, "html.parser")

    user=User(soup)
    a=user.followers
    print(user.followers)

    return a
async def enterUsernamesDB(list):
    cnx=mysql.connector.connect(user='user', password='',host='localhost',database='twitter')
    cursor = cnx.cursor(buffered=True)
    cursor.execute("Truncate table user")
    for i in list:
        
        cursor.execute("SELECT username, COUNT(*) FROM user WHERE username = %s GROUP BY username",(i,))
        # gets the number of rows affected by the command executed
        row_count = cursor.rowcount
        
        if row_count == 0:
            
            add_username = ("INSERT INTO user (username) VALUES (%s) ")
            
            d=(i,)
            
            
            cursor.execute(("SET FOREIGN_KEY_CHECKS=0"))
            cursor.execute(add_username,d ) 
            cnx.commit()
        
        

def scrapeUserInfo(r):
    json_response = loads(r)
    html = json_response["items_html"]

    soup = BeautifulSoup(html, "html.parser")
    
    feed = soup.find_all("div", "tweet")
    return feed, json_response["min_position"]



async def tweets(username,num):
    i=-1
    p=0
    cnx=mysql.connector.connect(user='user', password='',host='localhost',database='twitter',use_unicode=True,charset="utf8")
    cursor = cnx.cursor(buffered=True)
    
    while p<num:
        a=await Username(username,i,20)
        l,i=scrapeUserInfo(a)
        
        if len(l)>0:
            for j in l:
                
                t=Tweet(j)
                cursor.execute("SELECT tweetId, COUNT(*) FROM tweet WHERE tweetId = %s GROUP BY tweetId",(t.id,))
                row_count = cursor.rowcount
        
                if row_count == 0:
                    cursor.execute(("SET FOREIGN_KEY_CHECKS=0"))
                    tweet1 = ("SELECT tweetId FROM user WHERE username=%s")
                    b=(username,)
                    cursor.execute(tweet1,b)
                    a=cursor.fetchall()
                    
                    
                    tweet = ("INSERT INTO tweet (id,tweetId,datetime,timestamp,user_id,username,name,place,text,repliescount,retweetscount,likescount,link,text_html,video_links,imagelinks,links) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
                    d=(int(a[0][0]),int(t.id),str(t.datestamp),str(t.timestamp),str(t.user_id),str(t.username),str(t.name),str(t.place),str(t.text),int(t.replies_count),int(t.retweets_count),int(t.likes_count),str(t.link),str(t.text_html),str(t.video),str(t.photos),str(t.urls))
                    
                         

                
                    cursor.execute(tweet,d) 
                    cnx.commit()
                print(p)
                p=p+1
                if p>num:
                    break
                #y=[link.attrs["data-expanded-url"] for link in j.find_all('a',{'class':'twitter-timeline-link'}) if link.has_attr("data-expanded-url")]
                
                
               
         
            
    
async def user_info():
    
    a=await query_user_page()

    print(str(a))
def fillUsernameDb(list):
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(enterUsernamesDB(list))
def run(username,num):    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tweets(username,num))
