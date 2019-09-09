import json
import os
import time
import traceback
import boto3
from chalice import Chalice, Response
from cachetools import TTLCache
import requests

app = Chalice(app_name='hujjatytproxy')
CACHE = TTLCache(maxsize=10, ttl=30)
DYNAMODB_IF_STREAM_TTL = 600
DYNAMODB_IF_NO_STREAM_TTL = 60
BEFORE_GOING_OFFLINE = 600

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
                    Item={'channel': { 'S': channel}, 'time': {'N': str(time.time())}, 'result': {'S': json.dumps(result)}}, TableName=os.environ['TABLE'])
                print(result)
        except Exception as exc:
            print("Exception writing to dynamodb: %s" % (exc,))
            traceback.print_exc()
        return None

    @classmethod
    def request_from_youtube(cls, params):
        r = requests.get("https://www.googleapis.com/youtube/v3/search", params=params)
        try:
            result = r.json()
        except:
            result = r.text
        return r.status_code, result


def are_there_videos(result):
    if not result:
        return False
    return result.get('pageInfo', {}).get('totalResults', 0) > 0


def do_search_on_youtube(params, youtube_and_dynamodb=RealYoutubeDynamodb):
    channel = params.get("channelId")
    result = CACHE.get(channel)
    if result:
        return 200, result, 'cache'
    else:
        dresult = None
        decoded_dresult = None
        create_time = 0
        item = youtube_and_dynamodb.get_from_dynamodb(channel)
        try:
            if item:
                dresult = item.get('result', {}).get('S')
                create_time = item.get('time').get('N')
                if create_time and dresult:
                    create_time = int(float(create_time))
                    decoded_dresult = json.loads(dresult)
                    ttl = DYNAMODB_IF_STREAM_TTL if are_there_videos(decoded_dresult) else DYNAMODB_IF_NO_STREAM_TTL
                    if time.time() - create_time < ttl:
                        return 200, decoded_dresult, 'dynamodb'
        except Exception as exc:
            print("Exception decoding from dynamodb: %s" % (exc,))
            traceback.print_exc()
        try:
            if not params.get("key"):
                params["key"] = os.environ.get("YOUTUBE_API_KEY")
            status_code, result = youtube_and_dynamodb.request_from_youtube(params)
            if status_code == requests.codes.ok:
                if are_there_videos(decoded_dresult) and not are_there_videos(result) and time.time() - create_time < BEFORE_GOING_OFFLINE:
                    return 200, decoded_dresult, 'dynamodb'
                CACHE[channel] = result
                youtube_and_dynamodb.write_to_dynamodb(channel, result)
            else:
                if decoded_dresult:
                    return 200, decoded_dresult, 'dynamodb'
            return status_code, result, 'youtube'
        except Exception as exc:
            print(exc)
            return 500, {}, 'youtube'


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
    results = {}
    any_live = False
    for channel, id in CHANNELS.items():
        params = {}
        params.update(DEFAULT_PARAMS)
        params["channelId"] = id
        status_code, result, how = do_search_on_youtube(params)
        print("Retrieved %s from %s with status_code %d" % (channel, how, status_code))
        if status_code == requests.codes.ok:
            if len(result.get("items", [])) > 0:
                any_live = True
        results[channel] = {
            "status_code": status_code,
            "result": result}
    results["any_live"] = any_live
    return results

@app.route('/ping', cors=True)
def ping():
    return Response(body=json.dumps({}), status_code=200)