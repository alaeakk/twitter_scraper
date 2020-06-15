from time import strftime, localtime
from datetime import datetime
import json

import logging as logme



class tweet:
    """Define Tweet class
    """
    type = "tweet"

    def __init__(self):
        pass





def getText(tw):
    """
    get tweet's text
    """
    text = tw.find("p", "tweet-text").text
    text = text.replace("http", " http")
    text = text.replace("pic.twitter", " pic.twitter")

    return text

def getHtmlText(tw):
    """
    get html text 
    """
    
    text = tw.find("p", "tweet-text")
    return str(text)

def getStat(tw, _type):
    """Get stats about Tweet
    """
    logme.debug(__name__+':getStat')
    st = f"ProfileTweet-action--{_type} u-hiddenVisually"
    return tw.find("span", st).find("span")["data-tweet-stat-count"]

def getQuoteURL(tw):
    """Extract quote from tweet
    """
    logme.debug(__name__+':getQuoteURL')
    base_twitter = "https://twitter.com"
    quote_url = ""
    try:
        quote = tw.find("div","QuoteTweet-innerContainer")
        quote_url = base_twitter + quote.get("href")
    except:
        quote_url = ""

    return quote_url

def getRetweet(tw):
    """Get Retweet
    """
    
    
    _rt_object = tw.find('span', 'js-retweet-text')
    if _rt_object:
        _rt_id = _rt_object.find('a')['data-user-id']
        _rt_username = _rt_object.find('a')['href'][1:]
        
        return  _rt_id, _rt_username
    return '', ''
def getMentions(tw):
    """Extract mentions from tweet
    """
    logme.debug(__name__+':getMentions')
    try:
        mentions = tw["data-mentions"].split(" ")
    except:
        mentions = []

    return mentions
def Tweet(tw):
    print(str(tw).encode(encoding='utf_8', errors='strict'))
    #tweet object
    t = tweet()
    #tweet id 
    t.id = int(tw["data-item-id"])
    #tweet id string
    t.id_str = tw["data-item-id"]
    #conversation id
    t.conversation_id = tw["data-conversation-id"]
    #tweet's date
    t.datetime = int(tw.find("span", "_timestamp")["data-time-ms"])
    #tweet's datestamp
    t.datestamp = strftime("%Y-%m-%d", localtime(t.datetime/1000.0))
    #tweet's timestamp
    t.timestamp = strftime("%H:%M:%S", localtime(t.datetime/1000.0))
    #tweet's user id 
    t.user_id = int(tw["data-user-id"])
    #tweet's user id string
    t.user_id_str = tw["data-user-id"]
    #account's username
    t.username = tw["data-screen-name"]
    #hashtags list
    t.hashtags = [hashtag.text for hashtag in tw.find_all("a","twitter-hashtag")]
    #cashtags list
    t.cashtags = [cashtag.text for cashtag in tw.find_all("a", "twitter-cashtag")]
    #account's owner name
    t.name = tw["data-name"]
    #place of account owner
    t.place = tw.find("a","js-geo-pivot-link").text.strip() if tw.find("a","js-geo-pivot-link") else ""
    #timezone of the account
    t.timezone = strftime("%z", localtime())
    #encode emojis in text 
    for img in tw.findAll("img", "Emoji Emoji--forText"):
        a=img.replaceWith(img["alt"]+" ")
    #list of urls
    t.urls = [link.attrs["data-expanded-url"] for link in tw.find_all('a',{'class':'twitter-timeline-link'}) if link.has_attr("data-expanded-url")]
    #list of images
    t.photos = [photo_node.attrs['data-image-url'] for photo_node in tw.find_all("div", "AdaptiveMedia-photoContainer")]
    #is the tweet contains a video
    t.video = 1 if tw.find_all("div", "AdaptiveMedia-video") != [] else 0
    #list of users that the account's owner answered them 
    t.reply_to = [{'user_id': t['id_str'], 'username': t['screen_name']} for t in json.loads(tw["data-reply-to-users-json"])]
    #tweet text
    t.text = getText(tw)
    #retweet's username and id  
    t.user_rt_id, t.user_rt = getRetweet(tw)
    #true if the tweet is a retweet 
    t.retweet = True if t.user_rt else False
    #text html
    t.text_html=getHtmlText(tw)
    #quote's url
    t.quote_url = getQuoteURL(tw)
    #number of replies
    t.replies_count = getStat(tw, "reply")
    #number of retweets 
    t.retweets_count = getStat(tw, "retweet")
    #number of likes
    t.likes_count = getStat(tw, "favorite")
    #tweet's url
    t.link = f"https://twitter.com/{t.username}/status/{t.id}"
    #list of mentions
    t.mentions = getMentions(tw)
    
    return t