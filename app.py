import json
import os
import time
import traceback
import random
import boto3
from chalice import Chalice, Response
from cachetools import TTLCache
import requests
from durations import Duration
from durations.exceptions import ScaleFormatError, InvalidTokenError


app = Chalice(app_name='hujjatytproxy')
CACHE = TTLCache(maxsize=10, ttl=30)
DYNAMODB_IF_STREAM_TTL = 900
DYNAMODB_IF_NO_STREAM_TTL = 200
BEFORE_GOING_OFFLINE = 900
MIN_TIME_BEFORE_UPSTREAM_CHECKS = 200
MAX_GRACE_PERIOD = 100
GRACE_PERIOD = random.choice(range(MAX_GRACE_PERIOD))  # this is to for all nodes to not go and query at the same time

CHANNELS = {
    # "mainhall": "UCSSgKFdC-gRtxIgTrGGqP3g",
    "elc": "UCvjUFF1C3yO2KK17EjpiWGQ",
    # "ladies": "UCPOs_KwzIUfBq5sNR8IJynw"
}
DEFAULT_PARAMS = {
    "order": "date",
    "maxResults": "1",
    "type": "video",
    "eventType": "live",
    "safeSearch": "none",
    "videoEmbeddable": "true",
    "part": "snippet"
}


def reset_cache():
    global CACHE
    CACHE = TTLCache(maxsize=10, ttl=30)


def get_cache():
    return CACHE


def default_expiry(now):
    return now + BEFORE_GOING_OFFLINE


class RealYoutubeDynamodb:
    @classmethod
    def get_from_dynamodb(cls, channel):
        try:
            client = boto3.client('dynamodb')
            if 'TABLE' in os.environ:
                result = client.get_item(Key={'channel': {'S': channel}}, TableName=os.environ['TABLE'])
                if 'Item' in result:
                    return result['Item']
        except Exception as exc:
            print("Exception retrieving from dynamodb: %s" % (exc,))
            traceback.print_exc()
        return None

    @classmethod
    def write_to_dynamodb(cls, channel, result, expiry_time=None):
        try:
            client = boto3.client('dynamodb')
            if 'TABLE' in os.environ:
                now = time.time()
                if not expiry_time:
                    expiry_time = default_expiry(now)
                result = client.put_item(
                    Item={
                        'channel': {'S': channel},
                        'time': {'N': str(now)},
                        'last_checked_time': {'N': str(now)},
                        'expiry_time': {'N': str(expiry_time)},
                        'result': {'S': json.dumps(result)}},
                    TableName=os.environ['TABLE'])
        except Exception as exc:
            print("Exception writing to dynamodb: %s" % (exc,))
            traceback.print_exc()
        return None

    @classmethod
    def update_dynamodb(cls, channel, result, create_time, expiry_time=None):
        try:
            client = boto3.client('dynamodb')
            if 'TABLE' in os.environ:
                print(f"Updating dynamodb for {channel} with a current last_checked_time")
                if not expiry_time:
                    expiry_time = default_expiry(create_time)
                result = client.put_item(
                    Item={
                        'channel': {'S': channel},
                        'time': {'N': str(create_time)},
                        'last_checked_time': {'N': str(time.time())},
                        'expiry_time': {'N': str(expiry_time)},
                        'result': {'S': json.dumps(result)}},
                    TableName=os.environ['TABLE'])
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
        except json.decoder.JSONDecodeError:
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
        expiry_time = 0
        item = youtube_and_dynamodb.get_from_dynamodb(channel)
        try:
            if item:
                dresult = item.get('result', {}).get('S')
                create_time = item.get('time').get('N')
                expiry_time = item.get('expiry_time', {"N": default_expiry(int(float(create_time)))}).get('N')
                last_checked_time = item.get('last_checked_time', {"N": "0"}).get('N')
                if create_time and dresult:
                    create_time = int(float(create_time))
                    decoded_dresult = json.loads(dresult)
                    ttl = DYNAMODB_IF_STREAM_TTL if are_there_videos(decoded_dresult) else DYNAMODB_IF_NO_STREAM_TTL
                    now = time.time()
                    if now - create_time < ttl:
                        print(f"Using Dynamodb result as within ttl: now={now} create_time={create_time} ttl={ttl}")
                        return 200, decoded_dresult, 'dynamodb', None, None
        except Exception as exc:
            print("Exception decoding from dynamodb: %s" % (exc,))
            traceback.print_exc()
        return request_from_youtube_and_write_to_cache(
            params, decoded_dresult, create_time,
            int(float(last_checked_time)), int(float(expiry_time)), youtube_and_dynamodb)


def pick_youtube_api_key():
    keys = os.environ.get("YOUTUBE_API_KEYS", 'test_key:blah')
    all_keys = []
    for key in keys.split(','):
        origin, value = key.split(':')
        all_keys.append((origin, value))
    choice = random.choice(all_keys)
    return choice


def request_from_youtube_and_write_to_cache(params, decoded_dresult=None, create_time=0, last_checked_time=0,
                                            expiry_time=0, youtube_and_dynamodb=RealYoutubeDynamodb):
    channel = params.get("channelId")
    try:
        key_origin = None
        if not params.get("key"):
            key_origin, params["key"] = pick_youtube_api_key()
        else:
            key_origin = "provided_in_apicall"
        since_last_check = time.time() - last_checked_time
        if since_last_check > MIN_TIME_BEFORE_UPSTREAM_CHECKS + GRACE_PERIOD:
            print(f"Going to youtube to check because last check was done {since_last_check} ago")
            status_code, result = youtube_and_dynamodb.request_from_youtube(params, key_origin)
            if status_code == requests.codes.ok:
                now = time.time()
                if (are_there_videos(decoded_dresult) and not are_there_videos(result) and
                        expiry_time > now):
                    print(
                        f"Still using dynamodb result because youtube had no videos and "
                        f"{now - expiry_time} seconds left until expiry")
                    youtube_and_dynamodb.update_dynamodb(channel, decoded_dresult, create_time, expiry_time)
                    return (
                        200, decoded_dresult, 'dynamodb',
                        f"youtube status {status_code} with data {result}", key_origin)
                print(f"Caching result for {channel} from youtube")
                CACHE[channel] = result
                youtube_and_dynamodb.write_to_dynamodb(channel, result)
                return status_code, result, 'youtube', f"youtube status {status_code}", key_origin
            else:
                print(f"Youtube returned {status_code}")
                if decoded_dresult:
                    youtube_and_dynamodb.update_dynamodb(channel, decoded_dresult, create_time)
                    return (
                        200, decoded_dresult, 'dynamodb',
                        f"youtube status {status_code} with data {result}", key_origin)
                else:
                    return 500, {}, 'youtube', f"youtube status {status_code} with data {result}", key_origin
        else:
            print(f"Using dynamodb because last check was done {since_last_check} ago")
            if decoded_dresult:
                return 200, decoded_dresult, 'dynamodb', None, None
    except Exception as exc:
        print(exc)
        return 500, {}, 'youtube', f"error {exc}", key_origin


def force_video_id(video_id, channel, ttl, youtube_and_dynamodb=RealYoutubeDynamodb):
    channel_id = CHANNELS[channel]
    result = json.loads("""
    {
        "kind": "youtube#searchListResponse",
        "etag": "8jEFfXBrqiSrcF6Ee7MQuz8XuAM/IzvFVgDabZ_mOGJl5FfbgvJqoL4",
        "regionCode": "IE",
        "pageInfo": {
            "totalResults": 1,
            "resultsPerPage": 1
        },
        "items": [
            {
            "kind": "youtube#searchResult",
            "etag": "8jEFfXBrqiSrcF6Ee7MQuz8XuAM/fu3EinHUWmS-3Hb0Csc8-lKjhpk",
            "id": {
                "kind": "youtube#video",
                "videoId": "KTf2_GL_6lA"
            },
            "snippet": {
                "publishedAt": "2019-09-06T21:35:13.000Z",
                "channelId": "UCSSgKFdC-gRtxIgTrGGqP3g",
                "title": "The KSIMC of London - Stanmore - Main Hall Live Stream",
                "description": "",
                "thumbnails": {
                "default": {
                    "url": "https://i.ytimg.com/vi/KTf2_GL_6lA/default_live.jpg",
                    "width": 120,
                    "height": 90
                },
                "medium": {
                    "url": "https://i.ytimg.com/vi/KTf2_GL_6lA/mqdefault_live.jpg",
                    "width": 320,
                    "height": 180
                },
                "high": {
                    "url": "https://i.ytimg.com/vi/KTf2_GL_6lA/hqdefault_live.jpg",
                    "width": 480,
                    "height": 360
                }
                },
                "channelTitle": "The KSIMC of London - Stanmore - Main Hall",
                "liveBroadcastContent": "live"
            }
            }
        ]
        }""")
    result["items"][0]["id"]["videoId"] = video_id
    youtube_and_dynamodb.write_to_dynamodb(channel_id, result, time.time() + ttl)


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
        print(
            f"Retrieved {channel} from {how} with status_code {status_code} with info: {info}"
            f"and key origin: {key_origin}")
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
        result = client.get_item(Key={'channel': {'S': 'mainhall'}}, TableName=os.environ['TABLE'])
        client.put_item(
                    Item={
                        'channel': {'S': 'mainhall'},
                        'time': {'N': str(time.time())},
                        'result': {'S': json.dumps(result['Item'])}},
                    TableName=os.environ['TABLE'])
    return Response(body=json.dumps({}), status_code=200)


@app.route('/forcevideo', cors=True)
def force_video():
    video_id = app.current_request.query_params.get('videoId')
    channel = app.current_request.query_params.get('channel')
    ttl = app.current_request.query_params.get('ttl')
    try:
        duration = Duration(ttl).to_seconds()
    except (InvalidTokenError, ScaleFormatError, ValueError):
        return Response(body=json.dumps({}), status_code=400)
    if channel not in CHANNELS:
        return Response(body=json.dumps({}), status_code=400)
    if video_id:
        return force_video_id(video_id, channel, duration)
    else:
        return Response(body=json.dumps({}), status_code=400)
