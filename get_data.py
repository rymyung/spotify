
import sys
import requests
import base64
import json
import logging
import time
import pymysql

# Spotify API Info
client_id = ""
client_secret = ""

# AWS MySQL Info
host = ""
port = ""
username = ""
database = ""
password = ""


def main() :

    # Connect to AWS MySQL
    try :
        conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
        cursor = conn.cursor()

    except :
        logging.error("could not connect to MySQL")
        sys.exit(1)

    cursor.execute("SHOW TABLES")
    print(cursor.fetchall())


    # Get header
    headers = get_headers(client_id, client_secret)

    # Spotify Search API
    params = {
        "q" : "BTS",
        "type" : "artist",
        "limit" : "5"
    }

    r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)

    if r.status_code != 200 :
        logging.error(r.text)

        if r.status_code == 429 :
            retry_after = json.loads(r.headers)['Retry-After']
            time.sleep(int(retry_after))

            r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)

        # access_token expired
        elif r.status_code == 401 :
            headers = get_headers(client_id, client_secret)
            r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)

        else :
            sys.exit(1)

    # GET BTS' Albums

    r = requests.get("https://api.spotify.com/v1/artists/3Nrfpe0tUJi4K4DXYWgMUX/albums", headers=headers)

    raw = json.loads(r.text)

    total = raw['total']
    offset = raw['offset']
    limit = raw['limit']
    next = raw['next']

    albums = []
    albums.extend(raw['items'])

    count = 0
    while count < 100 and next :
        r = requests.get(raw['next'], headers=headers)
        raw = json.loads(r.text)
        next = raw['next']
        print(next)
        albums.extend(raw['items'])
        count = len(albums)

    print(albums[0])
    print(len(albums))
    sys.exit(0)




def get_headers(client_id, client_secret):

    endpoint = "https://accounts.spotify.com/api/token"
    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')

    headers = {
        "Authorization": "Basic {}".format(encoded)
    }

    payload = {
        "grant_type": "client_credentials"
    }

    r = requests.post(endpoint, data=payload, headers=headers)

    access_token = json.loads(r.text)['access_token']

    headers = {
        "Authorization": "Bearer {}".format(access_token)
    }

    return headers






if __name__=='__main__':
    main()