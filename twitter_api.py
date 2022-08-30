from dataclasses import dataclass
from typing import List

import requests

import db

token = ""
base_url = "https://api.twitter.com/2"
ole_birk_olesen = 2222188479


@dataclass
class TweetBaseInfo:
    id: str
    text: str


@dataclass
class TweetChunkMeta:
    next_token: str
    result_count: int
    newest_id: str
    oldest_id: str
    previous_token: str = None


@dataclass
class TweetChunk:
    data: List[TweetBaseInfo]
    meta: TweetChunkMeta


def get_tweet_chunk(pagination_token: str = None) -> TweetChunk:
    url = f"{base_url}/users/{ole_birk_olesen}/tweets"
    if pagination_token:
        url += f"?pagination_token={pagination_token}"
    response = requests.get(
        url=url,
        headers={"Authorization": f"Bearer {token}"}
    )
    return TweetChunk(
        data=[TweetBaseInfo(**tweet) for tweet in response.json()["data"]],
        meta=TweetChunkMeta(**response.json()["meta"])
    )


def fetch_all_base_info():
    pagination_token: str = db.get_latest_pagination_token()
    connection = db.open_connection()
    cursor = connection.cursor()
    while True:
        tweet_chunk: TweetChunk = get_tweet_chunk(pagination_token=pagination_token)
        pagination_token = tweet_chunk.meta.next_token
        cursor.execute(f"INSERT INTO PAGINATION_TOKEN (PAGINATION_TOKEN) VALUES ('{pagination_token}')")
        for tweet in tweet_chunk.data:
            text = tweet.text.replace("'", "")
            cursor.execute(f"INSERT INTO tweet_base_info (ID, TEXT) "
                           f"VALUES ('{tweet.id}', '{text}')")
        connection.commit()


def fix_next_tweet():
    connection = db.open_connection()
    tweet_id = connection.cursor().execute(
        "select ID from tweet_base_info where CREATED_AT is null ORDER BY ROWID DESC LIMIT 1"
    ).fetchone()[0]
    created_at = requests.get(
        url=f"{base_url}/tweets/{tweet_id}?tweet.fields=created_at",
        headers={"Authorization": f"Bearer {token}"}
    ).json()["data"]["created_at"].replace("T", " ").replace("Z", "")
    connection.cursor().execute(f"UPDATE tweet_base_info set CREATED_AT = '{created_at}' where id = '{tweet_id}'")
    connection.commit()

