import json
import os
import time
import traceback
import random
import boto3
from chalice import Chalice, Response
from cachetools import TTLCache
import requests

app = Chalice(app_name='hujjatytproxy')
CACHE = TTLCache(maxsize=10, ttl=30)
DYNAMODB_IF_STREAM_TTL = 600
DYNAMODB_IF_NO_STREAM_TTL = 100
BEFORE_GOING_OFFLINE = 600
MIN_TIME_BEFORE_UPSTREAM_CHECKS = 100

CHANNELS = {
    "mainhall": "UCSSgKFdC-gRtxIgTrGGqP3g",
    "elc": "UCvjUFF1C3yO2KK17EjpiWGQ",
    "ladies": "UCPOs_KwzIUfBq5sNR8IJynw"}
DEFAULT_PARAMS = {
    "order": "date",
    "maxResults": "1",
    "type": "video",
    "eventType": "live",
    "safeSearch": "none",
    "videoEmbeddable": "true",
    "part": "snippet"
}


class RealYoutubeDynamodb:
    @classmethod
    def get_from_dynamodb(cls, channel):
        try:
            client = boto3.client('dynamodb')
            if 'TABLE' in os.environ:
                result = client.get_item(Key={'channel': { 'S': channel}}, TableName=os.environ['TABLE'])
                if 'Item' in result:
                    return result['Item']
        except Exception as exc:
            print("Exception retrieving from dynamodb: %s" % (exc,))
            traceback.print_exc()
        return None

    @classmethod
    def write_to_dynamodb(cls, channel, result):
        try:
            client = boto3.client('dynamodb')
            if 'TABLE' in os.environ:
                result = client.put_item(
                    Item={'channel': { 'S': channel}, 'time': {'N': str(time.time())}, 'last_checked_time': {'N': str(time.time())}, 'result': {'S': json.dumps(result)}}, TableName=os.environ['TABLE'])
        except Exception as exc:
            print("Exception writing to dynamodb: %s" % (exc,))
            traceback.print_exc()
        return None

    @classmethod
    def update_dynamodb(cls, channel, result, create_time):
        try:
            client = boto3.client('dynamodb')
            if 'TABLE' in os.environ:
                print(f"Updating dynamodb for {channel} with a current last_checked_time")
                result = client.put_item(
                    Item={'channel': { 'S': channel}, 'time': {'N': str(create_time)}, 'last_checked_time': {'N': str(time.time())}, 'result': {'S': json.dumps(result)}}, TableName=os.environ['TABLE'])
        except Exception as exc:
            print("Exception writing to dynamodb: %s" % (exc,))
            traceback.print_exc()
        return None

    @classmethod
    def request_from_youtube(cls, params, key_origin):
        r = requests.get("https://www.googleapis.com/youtube/v3/search", params=params)
        print(f"Youtube API Request for {params.get('channelId')} with key origin {key_origin}")
        try:
            result = r.json()
        except:
            result = r.text
        print(f"Youtube API Response code: {r.status_code} for {params.get('channelId')} with key origin {key_origin}")
        return r.status_code, result


def are_there_videos(result):
    if not result:
        return False
    return result.get('pageInfo', {}).get('totalResults', 0) > 0


def do_search_on_youtube(params, youtube_and_dynamodb=RealYoutubeDynamodb):
    channel = params.get("channelId")
    result = CACHE.get(channel)
    if result:
        return 200, result, 'cache', None, None
    else:
        dresult = None
        decoded_dresult = None
        create_time = 0
        last_checked_time = 0
        item = youtube_and_dynamodb.get_from_dynamodb(channel)
        try:
            if item:
                dresult = item.get('result', {}).get('S')
                create_time = item.get('time').get('N')
                last_checked_time = item.get('last_checked_time', {"N": "0"}).get('N')
                if create_time and dresult:
                    create_time = int(float(create_time))
                    decoded_dresult = json.loads(dresult)
                    ttl = DYNAMODB_IF_STREAM_TTL if are_there_videos(decoded_dresult) else DYNAMODB_IF_NO_STREAM_TTL
                    if time.time() - create_time < ttl:
                        return 200, decoded_dresult, 'dynamodb', None, None
        except Exception as exc:
            print("Exception decoding from dynamodb: %s" % (exc,))
            traceback.print_exc()
        return request_from_youtube_and_write_to_cache(params, decoded_dresult, create_time, last_checked_time, youtube_and_dynamodb)


def pick_youtube_api_key():
    keys = os.environ.get("YOUTUBE_API_KEYS")
    all_keys = []
    for key in keys.split(','):
        origin, value = key.split(':')
        all_keys.append((origin, value))
    choice = random.choice(all_keys)
    return choice


def request_from_youtube_and_write_to_cache(params, decoded_dresult=None, create_time=0, last_checked_time=0, youtube_and_dynamodb=RealYoutubeDynamodb):
    channel = params.get("channelId")
    try:
        key_origin = None
        if not params.get("key"):
            key_origin, params["key"] = pick_youtube_api_key()
        else:
            key_origin = "provided_in_apicall"
        since_last_check = time.time() - int(float(last_checked_time))
        if since_last_check > MIN_TIME_BEFORE_UPSTREAM_CHECKS:
            print(f"Going to youtube to check because last check was done {since_last_check} ago")
            status_code, result = youtube_and_dynamodb.request_from_youtube(params, key_origin)
            if status_code == requests.codes.ok:
                if are_there_videos(decoded_dresult) and not are_there_videos(result) and time.time() - create_time < BEFORE_GOING_OFFLINE:
                    youtube_and_dynamodb.update_dynamodb(channel, decoded_dresult, create_time)
                    return 200, decoded_dresult, 'dynamodb', f"youtube status {status_code} with data {result}", key_origin
                CACHE[channel] = result
                youtube_and_dynamodb.write_to_dynamodb(channel, result)
                return status_code, result, 'youtube', f"youtube status {status_code}", key_origin
            else:
                if decoded_dresult:
                    youtube_and_dynamodb.update_dynamodb(channel, decoded_dresult, create_time)
                    return 200, decoded_dresult, 'dynamodb', f"youtube status {status_code} with data {result}", key_origin 
                else:
                    return 500, {}, 'youtube', f"youtube status {status_code} with data {result}", key_origin
        else:
            if decoded_dresult:
                return 200, decoded_dresult, 'dynamodb', None, None
    except Exception as exc:
        print(exc)
        return 500, {}, 'youtube', f"error {exc}", key_origin


@app.route('/v3/search', cors=True)
def youtube():
    status_code, result, how = do_search_on_youtube(app.current_request.query_params)
    if status_code == requests.codes.ok:
        return result
    else:
        return Response(
            body=json.dumps(result),
            status_code=status_code,
            headers={"Content-Type": "application/json"})


@app.route('/live', cors=True)
def any_live():
    return live(skip_cache=False)

@app.route('/refresh', cors=True)
def refresh_cache():
    return live(skip_cache=True)


def live(skip_cache=False):
    results = {}
    any_live = False
    for channel, id in CHANNELS.items():
        params = {}
        params.update(DEFAULT_PARAMS)
        params["channelId"] = id
        if skip_cache:
            status_code, result, how, info, key_origin = request_from_youtube_and_write_to_cache(params)
        else:
            status_code, result, how, info, key_origin = do_search_on_youtube(params)
        print(f"Retrieved {channel} from {how} with status_code {status_code} with info: {info} and key origin: {key_origin}")
        if status_code == requests.codes.ok:
            if len(result.get("items", [])) > 0:
                any_live = True
        results[channel] = {
            "status_code": status_code,
            "result": result,
            "how": how,
            "extra_info": info,
            "key_origin": key_origin}
    results["any_live"] = any_live
    return results

@app.route('/ping', cors=True)
def ping():
    if False:
        client = boto3.client('dynamodb')
        result = client.get_item(Key={'channel': { 'S': 'mainhall'}}, TableName=os.environ['TABLE'])
        client.put_item(
                    Item={'channel': { 'S': 'mainhall'}, 'time': {'N': str(time.time())}, 'result': {'S': json.dumps(result['Item'])}}, TableName=os.environ['TABLE'])
    return Response(body=json.dumps({}), status_code=200)