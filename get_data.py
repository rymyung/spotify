import sys
import requests
import base64
import json
import logging
import time

client_id = "313f33c5a29a4408bd001930d745b5c4a"
client_secret = "168f1767e63e41739292f56413c42c19"

def main() :
    headers = get_headers(client_id, client_secret)

    ## Spotify Search API
    params = {
        "q" : "BTS",
        "type" : "artist",
        "limit" : "5"
    }

    r = requests.get("https://api.spotify.com/v1/search", params = params, headers = headers)

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