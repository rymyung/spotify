
import sys
import requests
import base64
import json
import logging
import time
import pymysql
import csv

# Spotify API Info
client_id = "313f33c5a29a4408bd001930d745b5c4"
client_secret = "168f1767e63e41739292f56413c42c19"

# AWS MySQL Info
host = "spotify.cialxhlcrgzf.ap-northeast-2.rds.amazonaws.com"
port = 3306
username = "rymyung"
database = "production"
password = "du915aud"


def main() :

    # Connect to AWS MySQL
    try :
        conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
        cursor = conn.cursor()

    except :
        logging.error("could not connect to MySQL")
        sys.exit(1)


    # Get header
    headers = get_headers(client_id, client_secret)

    # Spotify Search API
    artists = []
    with open('artist_list.csv', encoding='utf8') as f:
        raw = csv.reader(f)
        for row in raw:
            artists.append(row[0].strip())

    for a in artists:

        params = {
            'q': a,
            'type': 'artist',
            'limit': '1'
        }

        r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)
        raw = json.loads(r.text)

        artist = {}
        try:
            artist_raw = raw['artists']['items'][0]
            if artist_raw['name'] == params['q']:
                artist.update(
                    {
                        'id': artist_raw['id'],
                        'name': artist_raw['name'],
                        'popularity': artist_raw['popularity'],
                        'url': artist_raw['external_urls']['spotify'],
                        'image_url': artist_raw['images'][0]['url'],
                        'followers': artist_raw['followers']['total']
                    }
                )
            insert_row(cursor, artist, 'artists')

        except:
            logging.error('No items from search API')
            continue

    conn.commit()
    sys.exit(0)




    if r.status_code != 200:
        logging.error(r.text)

        if r.status_code == 429:
            retry_after = json.loads(r.headers)['Retry-After']
            time.sleep(int(retry_after))

            r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)

        # access_token expired
        elif r.status_code == 401:
            headers = get_headers(client_id, client_secret)
            r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)

        else:
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


def insert_row(cursor, data, table):

    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())
    key_placeholders = ', '.join(['{0}=%s'.format(k) for k in data.keys()])
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)
    cursor.execute(sql, list(data.values())*2)



if __name__=='__main__':
    main()