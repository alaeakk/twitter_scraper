#!/usr/bin/env python


import argparse

import requests
import json
import urllib.parse
import m3u8
from pathlib import Path
import re
import sys
import ffmpeg
import shutil


class VideoUrl:

    video_player_prefix = 'https://twitter.com/i/videos/tweet/'
    video_api = 'https://api.twitter.com/1.1/videos/tweet/config/'
    tweet_data = {}

    def __init__(self, tweet_url,debug = 0):
        self.tweet_url = tweet_url
        
        self.debug = debug

        if debug > 2:
            self.debug = 2


        self.tweet_data['tweet_url'] = tweet_url.split('?', 1)[0]
        self.tweet_data['user'] = self.tweet_data['tweet_url'].split('/')[3]
        self.tweet_data['id'] = self.tweet_data['tweet_url'].split('/')[5]

       
        
        
        self.requests = requests.Session()

    def geturl(self):
        self.__debug('Tweet URL', self.tweet_data['tweet_url'])

        # Get the bearer token
        token = self.__get_bearer_token()

        # Get the M3u8 file - this is where rate limiting has been happening
        video_host, playlist = self.__get_playlist(token)
        print(video_host)
        if playlist.is_variant:
            plist=playlist.playlists[0]
            resolution = str(plist.stream_info.resolution[0]) + 'x' + str(plist.stream_info.resolution[1])
            resolution_file = Path(self.storage) / Path(resolution + '.mp4')
                
                

            playlist_url = video_host + plist.uri
        return str(playlist_url)
                

    def __get_bearer_token(self):
        video_player_url = self.video_player_prefix + self.tweet_data['id']
        video_player_response = self.requests.get(video_player_url).text
        self.__debug('Video Player Body', '', video_player_response)

        js_file_url  = re.findall('src="(.*js)', video_player_response)[0]
        js_file_response = self.requests.get(js_file_url).text
        self.__debug('JS File Body', '', js_file_response)

        bearer_token_pattern = re.compile('Bearer ([a-zA-Z0-9%-])+')
        bearer_token = bearer_token_pattern.search(js_file_response)
        bearer_token = bearer_token.group(0)
        self.requests.headers.update({'Authorization': bearer_token})
        self.__debug('Bearer Token', bearer_token)
        self.__get_guest_token()

        return bearer_token


    def __get_playlist(self, token):
        player_config_req = self.requests.get(self.video_api + self.tweet_data['id'] + '.json')

        player_config = json.loads(player_config_req.text)

        if 'errors' not in player_config:
            self.__debug('Player Config JSON', '', json.dumps(player_config))
            m3u8_url = player_config['track']['playbackUrl']

        else:
            self.__debug('Player Config JSON - Error', json.dumps(player_config['errors']))
            print('[-] Rate limit exceeded. Could not recover. Try again later.')
            sys.exit(1)

        # Get m3u8
        m3u8_response = self.requests.get(m3u8_url)
        self.__debug('M3U8 Response', '', m3u8_response.text)

        m3u8_url_parse = urllib.parse.urlparse(m3u8_url)
        video_host = m3u8_url_parse.scheme + '://' + m3u8_url_parse.hostname

        m3u8_parse = m3u8.loads(m3u8_response.text)

        return [video_host, m3u8_parse]


    
    def __get_guest_token(self):
        res = self.requests.post("https://api.twitter.com/1.1/guest/activate.json")
        res_json = json.loads(res.text)
        self.requests.headers.update({'x-guest-token': res_json.get('guest_token')})


    def __debug(self, msg_prefix, msg_body, msg_body_full = ''):
        if self.debug == 0:
            return

        if self.debug == 1:
            print('[Debug] ' + '[' + msg_prefix + ']' + ' ' + msg_body)

        if self.debug == 2:
            print('[Debug+] ' + '[' + msg_prefix + ']' + ' ' + msg_body + ' - ' + msg_body_full)



