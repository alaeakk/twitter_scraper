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
    
    text = tw.find("p", "tweet-text").text
    text = text.replace("http", " http")
    text = text.replace("pic.twitter", " pic.twitter")

    return text

def getHtmlText(tw):
    
    text = tw.find("p", "tweet-text")
    return str(text)

def getStat(tw, _type):
    """Get stats about Tweet
    """
    logme.debug(__name__+':getStat')
    st = f"ProfileTweet-action--{_type} u-hiddenVisually"
    return tw.find("span", st).find("span")["data-tweet-stat-count"]



def Tweet(tw):
    t = tweet()
    t.id = int(tw["data-item-id"])
    t.id_str = tw["data-item-id"]
    t.conversation_id = tw["data-conversation-id"]
    t.datetime = int(tw.find("span", "_timestamp")["data-time-ms"])
    t.datestamp = strftime("%Y-%m-%d", localtime(t.datetime/1000.0))
    t.timestamp = strftime("%H:%M:%S", localtime(t.datetime/1000.0))
    t.user_id = int(tw["data-user-id"])
    t.user_id_str = tw["data-user-id"]
    t.username = tw["data-screen-name"]
    t.hashtags = [hashtag.text for hashtag in tw.find_all("a","twitter-hashtag")]
    t.name = tw["data-name"]
    t.place = tw.find("a","js-geo-pivot-link").text.strip() if tw.find("a","js-geo-pivot-link") else ""
    t.timezone = strftime("%z", localtime())
    for img in tw.findAll("img", "Emoji Emoji--forText"):
        img.replaceWith(img["alt"])
    t.urls = [link.attrs["data-expanded-url"] for link in tw.find_all('a',{'class':'twitter-timeline-link'}) if link.has_attr("data-expanded-url")]
    t.photos = [photo_node.attrs['data-image-url'] for photo_node in tw.find_all("div", "AdaptiveMedia-photoContainer")]
    t.video = 1 if tw.find_all("div", "AdaptiveMedia-video") != [] else 0
    
    t.text = getText(tw)
    t.text_html=getHtmlText(tw)
    t.replies_count = getStat(tw, "reply")
    t.retweets_count = getStat(tw, "retweet")
    t.likes_count = getStat(tw, "favorite")
    t.link = f"https://twitter.com/{t.username}/status/{t.id}"
    
    return t