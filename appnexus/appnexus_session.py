'''
Created on May 27, 2013

@author: dtheodor
'''
import json
import urllib2

import requests

import cookie_persistance as cookie_p


def authenticate_to_appnexus(auth_data, session=None):
    """return cookie with authentication token, valid for 2 hours
    """
    headers = {"Content-type": "application/json; charset=UTF-8"} 
    url = "https://api.appnexus.com/auth"     
    data = json.dumps(auth_data)
    if session:
        r = session.post(url, data=data, headers=headers)
    else:
        r = requests.post(url, data=data, headers=headers)
    r.raise_for_status()
    return r.cookies

class AppnexusSession(object):
    """An Appnexus API session that provides helper functions to get appnexus
    data. Takes care of authorization.
    """
    def __init__(self, auth_data, cookie_file=None):
        self.session = requests.Session()
        self.auth_data = auth_data
        if cookie_file:
            self.session.cookies = cookie_p.load_cookies(cookie_file)        
        
    def _appnexus_auth(fn):
        """decorator to use with calls to appnexus. On authentication error
        it will try to authenticate and retry the call"""
        def wrapper(self, *args, **kwargs):
            try:
                return fn(self, *args, **kwargs)
            except requests.HTTPError as ex:
                print ex
                if ex.response.status_code == 401:#does ex always have a response?
                    if ex.response.json()['response']['error_id'] == 'NOAUTH':
                        authenticate_to_appnexus(auth_data, self.session)
                        print "authenticated, trying again"
                        return fn(self, *args, **kwargs)
        return wrapper

    def get_adservers(self):
        return self.get('https://api.appnexus.com/adserver')

    def get_members(self):
        return self.get("https://api.appnexus.com/member")

    def get_loglevel_feed(self, feed_name=None, hour=None):
        """Get log-level feed information.
        Feed name can be: 'standard_feed', 'segment_feed', 'bid_landscape_feed'
        hour must be in the 'YYYY_MM_DD_HH' format
        """
        url = "https://api.appnexus.com/siphon"  
        params = {}
        if feed_name:
            params['siphon_name'] = feed_name
        if hour:
            params['hour'] = hour
        return self.get(url, params=params)   
    
    def get_loglevel_standardfeed(self, hour=None):
        """Get log-level feed information.
        Feed name can be: 'standard_feed', 'segment_feed', 'bid_landscape_feed'
        hour must be in the 'YYYY_MM_DD_HH' format
        """
        return self.get_loglevel_feed('standard_feed', hour)
        
    def get_feed_download_url(self, name, hour, timestamp, split_part):
        """Make a GET call to the siphon-download service to request the
        location from which you can download the file.
        Include the siphon_name, hour, timestamp, and split_part in the query
        string of the call.
        
        e.g.
        https://api.appnexus.com/siphon-download?siphon_name=standard_feed&hour=2012_02_14_00&timestamp=20120214022600&split_part=0'
        """
        url = "https://api.appnexus.com/siphon-download"   
        params = dict(siphon_name=name,
                      hour=hour,
                      timestamp=timestamp,
                      split_part=split_part)
        #returns a redirect 302 so we need to disable redirect
        r = self.get(url, params=params, allow_redirects=False)
        return r.headers['location']
        
    
    @_appnexus_auth
    def get(self, url, **kwargs):
        """Appnexus GET call"""
        r = self.session.get(url, **kwargs)
        r.raise_for_status()
        return r

    _appnexus_auth = staticmethod(_appnexus_auth)
    

if __name__ == "__main__":
    
    print "Loading auth data..."
    auth_data = None
    with open('appnexus_auth', 'rb') as f:
        auth_data = json.load(f)    
    
    print "Starting appnexus session..."
    s = AppnexusSession(auth_data, cookie_file='appnexus_cookies')
    
    name = 'standard_feed'
    hour = '2013_05_31_17'
    print "Requesting standard feed..."
    r = s.get_loglevel_feed(name, hour)
    
    found_feeds = r.json()['response']['siphons']
    timestamp = found_feeds[0]['timestamp']
    splits = found_feeds[0]['splits']
    
    download_urls = []
    for split in splits:
        print "Getting url for {0} {1} {2} {3}...".format(name, hour, timestamp, split['part'])
        download_urls.append(s.get_feed_download_url(name, hour, timestamp, split['part']))
    i = 0
    for download_url in download_urls:
        print "Downloading {0}...".format(download_url)
        r = s.get(download_url, stream=True)
        with open("partial{0}".format(i), "ab") as myfile:
            for chunk in r.iter_content(chunk_size=1024):
                print chunk
                myfile.write(chunk)
        i += 1


        
    



