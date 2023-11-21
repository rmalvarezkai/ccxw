"""
CCXW - CryptoCurrency eXchange Websocket Library
Common functions used by the package

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
"""

import urllib.parse
import urllib.request
import json

def is_json(myjson):
    """
    is_json
    =======
        This function get a string or bytes and check if json return True
        if the input is a json valid string.
            :param myjson: str | bytes.

            :return bool: Return True if myjson is a str and is a json. 
    """

    result = False

    if isinstance(myjson,(str,bytes)):
        try:
            json.loads(myjson)
            result = True
        except Exception: # pylint: disable=broad-except
            result = False

    return result

def file_get_contents_url(url,mode="b",post_data=None,headers=None,timeout=9):
    """
    file_get_contents_url
    =====================
        This function get a url and reads into a string
            :param url: str file URL.
            :param mode: str "b" for binary response.
            :param post_data: dict Post data in format key -> value.
            :param headers: dict headers in format key -> value.
            :param timeout: int request timeout.

            :return str: Return response data from url. 
    """

    result = None

    if headers is None:
        headers = {}

    try:
        req = None
        if post_data is not None and isinstance(post_data,dict):
            req = urllib.request.Request(url, urllib.parse.urlencode(post_data).encode(),headers)
        else:
            req = urllib.request.Request(url, None,headers)

        if req is not None:
            try:
                with urllib.request.urlopen(req, None, timeout=timeout) as response:
                    result = response.read()

            except Exception: # pylint: disable=broad-except
                result = None

        if result is not None and isinstance(result,bytes):
            result = result.decode()

    except Exception: # pylint: disable=broad-except
        result = None

    if mode != "b" and result is not None and result is not False and isinstance(result,bytes):
        result = result.decode()

    return result
