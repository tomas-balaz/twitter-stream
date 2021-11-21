import requests
import os
from dotenv import load_dotenv
import json
import re

from functools import reduce

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
load_dotenv()
bearer_token = os.environ.get("BEARER_TOKEN")


def create_url():
    return "https://api.twitter.com/2/tweets/sample/stream"


def get_query_params():
    # user, time, place, referenced tweets (retweeted, quoted, replied_to), context/entities,
    # hashtags, public metrics (likes, replies, retweets), language, source (device),…

    query_params = {
        'tweet.fields':
            'author_id,created_at,text,id,lang,public_metrics,referenced_tweets,source',
        'user.fields':
            'username,created_at,id,public_metrics',
        'expansions':
            'author_id,referenced_tweets.id'
    }
    return query_params


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2SampledStreamPython"
    return r


def get_rf_tweet_and_user(json_response):
    tweet_data = json_response["data"]
    users_data = json_response["includes"]["users"]
    rf_tweet, rf_user_data = None, None

    if "referenced_tweets" in tweet_data:
        if "tweets" not in json_response["includes"]:
            return rf_tweet, rf_user_data
        tweets = json_response["includes"]["tweets"]
        rf_tweet = next(filter(lambda t: (t["id"] == tweet_data["referenced_tweets"][0]["id"]), tweets))
        rf_user_data = next(filter(lambda u: (u["id"] == rf_tweet["author_id"]), users_data))

    return rf_tweet, rf_user_data


def get_rf_username(tweet_data, rf_user_data):
    tweet_text = tweet_data["text"].replace('\n', ' ')
    rf_tweet_type = tweet_data['referenced_tweets'][0]['type']
    if rf_tweet_type == 'retweeted':
        return re.search(r'^RT @(.+): ', tweet_text)[1]
    # elif rf_tweet_type == 'replied_to':
    #     return re.search(r'^@(\S+) ', tweet_text)[1]
    else:
        return rf_user_data['username']


def get_tweet_data(tweet_data, author_data):
    data = {
        'id': tweet_data["id"],
        'created_at': tweet_data["created_at"],
        'lang': tweet_data["lang"],
        'source': tweet_data["source"],
        'text': tweet_data["text"].replace('\n', ' '),
        'user_id': tweet_data["author_id"],
        'user_name': author_data["username"],
        'registered': author_data["created_at"],
        'followers': author_data["public_metrics"]["followers_count"],
        'following': author_data["public_metrics"]["following_count"],
        'tweets': author_data["public_metrics"]["tweet_count"],
    }
    return data


def get_referenced_tweet_data(tweet_data, rf_tweet, rf_user_data):

    rt_data = {
        'ref_tweet_id': tweet_data['referenced_tweets'][0]['id'],
        'rf_type': tweet_data['referenced_tweets'][0]['type'],
        'rf_created_at': rf_tweet['created_at'],
        'rf_lang': rf_tweet['lang'],
        'rt_source': rf_tweet['source'],
        'rt_text': rf_tweet['text'].replace('\n', ' '),
        'rt_likes': rf_tweet['public_metrics']['like_count'],
        'rt_quotes': rf_tweet['public_metrics']['quote_count'],
        'rt_replies': rf_tweet['public_metrics']['reply_count'],
        'rt_retweets': rf_tweet['public_metrics']['retweet_count'],
        'rt_user_id': rf_tweet['author_id'],
        # 'rt_user_name': rf_user_data['username'],
        'rt_user_name': get_rf_username(tweet_data, rf_user_data),
        'rt_registered': rf_user_data['created_at'],
        'rt_followers': rf_user_data['public_metrics']['followers_count'],
        'rt_following': rf_user_data['public_metrics']['following_count'],
        'rt_tweets': rf_user_data['public_metrics']['tweet_count']
    }
    return rt_data


def json_to_csv(json_response):
    tweet_data = json_response["data"]
    users_data = json_response["includes"]["users"]

    if tweet_data["lang"] != "en":
        return None

    author_data, rf_user_data = {}, {}
    for u in users_data:
        if u["id"] == tweet_data["author_id"]:
            author_data = u

    rf_tweet, rf_user_data = get_rf_tweet_and_user(json_response)

    data = get_tweet_data(tweet_data, author_data)

    if rf_tweet:
        rt_data = get_referenced_tweet_data(tweet_data, rf_tweet, rf_user_data)
        data.update(rt_data)
    csv_line = list(reduce(lambda x, y: x + y, data.items()))
    csv_line = '\t'.join(map(str, csv_line))

    return csv_line + '\t\n'


def process_response(response, saved_tweets_count):
    with open(f"csv_data_{saved_tweets_count}_tweets.csv", "a", encoding='utf-8') as csv_file:
        lines_count = 0
        for response_line in response.iter_lines():
            if lines_count == saved_tweets_count:
                print(f'{saved_tweets_count} tweets was saved into CSV file')
                break

            if response_line:
                json_response = json.loads(response_line)
                # print(json.dumps(json_response, indent=4, sort_keys=True))
                csv_line = json_to_csv(json_response)
                if csv_line:
                    # print(csv_line)
                    csv_file.write(csv_line)
                    if lines_count % 100 == 0:
                        print(f'{lines_count} / {saved_tweets_count}')
                    lines_count += 1


def connect_to_endpoint(url, query_params):
    response = requests.request("GET", url, auth=bearer_oauth, stream=True, params=query_params)
    print(f'Response Status Code: {response.status_code}\n')
    if response.status_code == 200:
        process_response(response, saved_tweets_count=250000)
    else:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )


def main():
    url = create_url()
    query_params = get_query_params()
    connect_to_endpoint(url, query_params)


if __name__ == "__main__":
    main()
