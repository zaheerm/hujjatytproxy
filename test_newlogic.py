import boto3
import traceback
import json
import time
from cachetools import TTLCache
import requests
import unittest
import os
import mock

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


class FakeYoutubeDynamodb:
    @classmethod
    def request_from_youtube_online(cls, params):
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
        return 200, result

    @classmethod
    def request_from_youtube_offline(cls, params):
        result = """
    {
        "kind": "youtube#searchListResponse",
        "etag": "8jEFfXBrqiSrcF6Ee7MQuz8XuAM/nxmhARCBNQQrPOpwtM0UNWBXCsg",
        "regionCode": "IE",
        "pageInfo": {
            "totalResults": 0,
            "resultsPerPage": 1
        },
        "items": []
        }"""
        return 200, json.loads(result)

    request_from_youtube = request_from_youtube_online

    @classmethod
    def get_from_dynamodb(cls, channel):
        return None
    
    @classmethod
    def write_to_dynamodb(cls, channel, result):
        return None


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


class TestYoutubeSearch(unittest.TestCase):
    def setUp(self):
        global CACHE
        CACHE = TTLCache(maxsize=10, ttl=30)
        self.params = DEFAULT_PARAMS.copy()
        self.params['channelId'] = CHANNELS['mainhall']


    def test_local_cache(self):
        _status, _, where = do_search_on_youtube(self.params, FakeYoutubeDynamodb)
        self.assertEqual(len(CACHE), 1)
        self.assertEqual(where, 'youtube')
        _status, _, where = do_search_on_youtube(self.params, FakeYoutubeDynamodb)
        self.assertEqual(len(CACHE), 1)
        self.assertEqual(where, 'cache')

    def test_from_dynamodb_after_if_stream_ttl_uses_youtube(self):
        with mock.patch.object(FakeYoutubeDynamodb, 'get_from_dynamodb') as get_from_dynamodb_mock:
            get_from_dynamodb_mock.return_value = {
                'result': {'S': json.dumps(FakeYoutubeDynamodb.request_from_youtube_online(1)[1])},
                'time': {'N': str(time.time() - DYNAMODB_IF_STREAM_TTL - 1)}}
            _status, _, where = do_search_on_youtube(self.params, FakeYoutubeDynamodb)
            self.assertEqual(where, 'youtube')

    def test_from_dynamodb_before_if_stream_ttl(self):
        with mock.patch.object(FakeYoutubeDynamodb, 'get_from_dynamodb') as get_from_dynamodb_mock:
            get_from_dynamodb_mock.return_value = {
                'result': {'S': json.dumps(FakeYoutubeDynamodb.request_from_youtube_online(1)[1])},
                'time': {'N': str(time.time() - DYNAMODB_IF_STREAM_TTL + 1)}}
            _status, _, where = do_search_on_youtube(self.params, FakeYoutubeDynamodb)
            self.assertEqual(where, 'dynamodb')

    def test_from_dynamodb_after_if_no_stream_ttl_uses_youtube(self):
        with mock.patch.object(FakeYoutubeDynamodb, 'get_from_dynamodb') as get_from_dynamodb_mock:
            get_from_dynamodb_mock.return_value = {
                'result': {'S': json.dumps(FakeYoutubeDynamodb.request_from_youtube_offline(1)[1])},
                'time': {'N': str(time.time() - DYNAMODB_IF_NO_STREAM_TTL - 1)}}
            _status, _, where = do_search_on_youtube(self.params, FakeYoutubeDynamodb)
            self.assertEqual(where, 'youtube')

    def test_from_dynamodb_before_if_no_stream_ttl(self):
        with mock.patch.object(FakeYoutubeDynamodb, 'get_from_dynamodb') as get_from_dynamodb_mock:
            get_from_dynamodb_mock.return_value = {
                'result': {'S': json.dumps(FakeYoutubeDynamodb.request_from_youtube_offline(1)[1])},
                'time': {'N': str(time.time() - DYNAMODB_IF_NO_STREAM_TTL + 1)}}
            _status, _, where = do_search_on_youtube(self.params, FakeYoutubeDynamodb)
            self.assertEqual(where, 'dynamodb')


if __name__ == "__main__":
    unittest.main()