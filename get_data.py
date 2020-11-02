
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

    cursor.execute("SELECT id FROM artists")
    artists = []
    for (id, ) in cursor.fetchall():
        artists.append(id)

    artist_batch = [artists[i: i+50] for i in range(0, len(artists), 50)]

    artist_genres = []

    for i in artist_batch:
        ids = ','.join(i)
        url = 'https://api.spotify.com/v1/artists/?ids={}'.format(ids)

        r = requests.get(url, headers=headers)
        raw = json.loads(r.text)

        for artist in raw['artists']:

            for genre in artist['genres']:
                artist_genres.append(
                    {
                        'artist_id':artist['id'],
                        'genre':genre
                    }
                )

    for data in artist_genres:
        insert_row(cursor, data, 'artist_genres')

    conn.commit()
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


def insert_row(cursor, data, table):

    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())
    key_placeholders = ', '.join(['{0}=%s'.format(k) for k in data.keys()])
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)
    cursor.execute(sql, list(data.values())*2)



if __name__=='__main__':
    main()