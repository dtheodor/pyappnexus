'''
Created on May 31, 2013

@author: dtheodor

Save and restore cookies from files.
'''
import pickle
import cookielib

import requests

def save_cookies(requests_cookiejar, filename):
    """Pickles the RequestsCookieJar to the filename"""
    with open(filename, 'wb') as f:
        pickle.dump(requests_cookiejar, f)
        
def load_cookies(filename):
    """Loads and returns a RequestsCookieJar from the pickle file."""
    with open(filename, 'rb') as f:
        requests_cookiejar = pickle.load(f)
    return requests_cookiejar
        
def save_cookies_lwp(cookiejar, filename):
    """Saves the cookiejar to a file with 
    libwww-perl Set-Cookie3 human-readable format"""
    lwp_cookiejar = cookielib.LWPCookieJar()
    for c in cookiejar:
        argz = dict(vars(c).items())
        argz['rest'] = argz['_rest']
        del argz['_rest']
        c = cookielib.Cookie(**argz)
        lwp_cookiejar.set_cookie(c)
    lwp_cookiejar.save(filename, ignore_discard=True)
    
def load_cookies_from_lwp(filename):
    """Load a cookiejar from a libwww-perl Set-Cookie3 human-readable file"""
    lwp_cookiejar = cookielib.LWPCookieJar()
    lwp_cookiejar.load(filename, ignore_discard=True)
    return lwp_cookiejar