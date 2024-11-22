"""
CCXW - CryptoCurrency eXchange Websocket Library
Common functions used by the package

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
"""

import urllib.parse
import urllib.request
import json
import socket
import random
import string

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

    if myjson is not None and isinstance(myjson,(str,bytes)):
        try:
            json.loads(myjson)
            result = True
        except Exception: # pylint: disable=broad-except
            result = False

    return result

def is_convertible_to_json(myvar):
    """
    is_convertible_to_json
    ======================
        This function check if myvar is convertible to json string.
            :param myvar:

            :return bool: Return True if myvar is convertible to json string.
    """

    result = False
    try:
        json.dumps(myvar)
        result = True
    except Exception: # pylint: disable=broad-except
        result = False

    return result

def random_string(n=14):
    """
    random_string
    =============
    This function returns a random string of the specified length.

    :param n: int Length of the resulting string (default is 14).

    :return str: A randomly generated string of length 'n'.
    """
    letters = string.ascii_letters + '0123456789'
    return ''.join(random.choice(letters) for _ in range(n))

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

def file_put_contents(filename, data, mode_in=""):
    """
    file_put_contents
    =================
    This function writes the given data to the specified file.

    :param filename: str, the file path where data will be written.
    :param data: str, the content to write into the file.
    :param mode_in: str, optional, 'b' for binary mode. Defaults to "".

    :return: bool, returns True if the operation was successful, False otherwise.
    """
    result = False
    mode = "w"

    if len(mode_in) > 0:
        mode = mode_in
    try:
        f = open(filename, mode, encoding='utf-8')
        result = f.write(data)
        f.close()
    except Exception: # pylint: disable=broad-except
        result = False

    return result

def is_port_free(port, host='localhost'):
    """
    is_port_free
    ============
        This function check if port already used
            :param port: int port number.
            :param host: str 

            :return bool: Return True if port is free
    """
    result = False

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((host, port))
        s.close()
        result = True
    except Exception: # pylint: disable=broad-except
        result = False

    return result
