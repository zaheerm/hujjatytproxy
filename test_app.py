import json
import time
import copy
from unittest import mock
import pytest
from app import do_search_on_youtube, DYNAMODB_IF_STREAM_TTL, MIN_TIME_BEFORE_UPSTREAM_CHECKS
from app import CHANNELS, DEFAULT_PARAMS, MAX_GRACE_PERIOD, reset_cache, get_cache
from app import DYNAMODB_IF_NO_STREAM_TTL, force_video_id


class FakeYoutubeDynamodb:
    @classmethod
    def request_from_youtube_online(cls, params, key_origin):
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
    def request_from_youtube_offline(cls, params, key_origin):
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
    def write_to_dynamodb(cls, channel, result, expiry_time=None):
        return None

    @classmethod
    def update_dynamodb(cls, channel, result, create_time, expiry_time=None):
        return None


@pytest.fixture
def search_params():
    params = copy.deepcopy(DEFAULT_PARAMS)
    params.update({"channelId": list(CHANNELS.values())[0]})
    return params


def setup_function(function):
    print("Resetting cache")
    reset_cache()


def test_local_cache(search_params):
    _status, _, where, status_text, _ = do_search_on_youtube(search_params, FakeYoutubeDynamodb)
    print(status_text)
    assert where == 'youtube'
    assert len(get_cache()) == 1
    _status, _, where, _, _ = do_search_on_youtube(search_params, FakeYoutubeDynamodb)
    assert where == 'cache'
    assert len(get_cache()) == 1


def test_after_ttl_expires_uses_youtube(search_params):
    with mock.patch.object(FakeYoutubeDynamodb, 'get_from_dynamodb') as get_from_dynamodb_mock:
        get_from_dynamodb_mock.return_value = {
            'result': {'S': json.dumps(FakeYoutubeDynamodb.request_from_youtube_online(1, None)[1])},
            'time': {'N': str(time.time() - DYNAMODB_IF_STREAM_TTL - 1)},
            'expiry_time': {'N': str(time.time() - 1)},
            'last_checked_time': {'N': str(time.time() - MIN_TIME_BEFORE_UPSTREAM_CHECKS - MAX_GRACE_PERIOD - 1)}}
        _status, _, where, _, _ = do_search_on_youtube(search_params, FakeYoutubeDynamodb)
        assert where == 'youtube'


def test_after_if_stream_ttl_expires_but_video_expiry_not_reached_checks_youtube_but_returns_dynamodb(search_params):
    with mock.patch.object(FakeYoutubeDynamodb, 'get_from_dynamodb') as get_from_dynamodb_mock:
        # make youtube return no videos
        with mock.patch.object(FakeYoutubeDynamodb, 'request_from_youtube') as request_from_youtube_mock:
            request_from_youtube_mock.return_value = FakeYoutubeDynamodb.request_from_youtube_offline(1, None)
            get_from_dynamodb_mock.return_value = {
                'result': {'S': json.dumps(FakeYoutubeDynamodb.request_from_youtube_online(1, None)[1])},
                'time': {'N': str(time.time() - DYNAMODB_IF_STREAM_TTL - 1)},
                'expiry_time': {'N': str(time.time() + 1)},
                'last_checked_time': {'N': str(time.time() - MIN_TIME_BEFORE_UPSTREAM_CHECKS - MAX_GRACE_PERIOD - 1)}}
            _status, _, where, _, _ = do_search_on_youtube(search_params, FakeYoutubeDynamodb)
            assert where == 'dynamodb'
            assert request_from_youtube_mock.called


def test_after_if_stream_ttl_expires_but_video_expiry_not_reached_youtube_has_videos_returns_youtube(search_params):
    with mock.patch.object(FakeYoutubeDynamodb, 'get_from_dynamodb') as get_from_dynamodb_mock:
        get_from_dynamodb_mock.return_value = {
            'result': {'S': json.dumps(FakeYoutubeDynamodb.request_from_youtube_online(1, None)[1])},
            'time': {'N': str(time.time() - DYNAMODB_IF_STREAM_TTL - 1)},
            'expiry_time': {'N': str(time.time() + 1)},
            'last_checked_time': {'N': str(time.time() - MIN_TIME_BEFORE_UPSTREAM_CHECKS - MAX_GRACE_PERIOD - 1)}}
        _status, _, where, _, _ = do_search_on_youtube(search_params, FakeYoutubeDynamodb)
        assert where == 'youtube'


def test_before_ttl_expires_uses_dynamodb(search_params):
    with mock.patch.object(FakeYoutubeDynamodb, 'get_from_dynamodb') as get_from_dynamodb_mock:
        get_from_dynamodb_mock.return_value = {
            'result': {'S': json.dumps(FakeYoutubeDynamodb.request_from_youtube_online(1, None)[1])},
            'time': {'N': str(time.time() - DYNAMODB_IF_STREAM_TTL + 1)},
            'last_checked_time': {'N': str(time.time() - MIN_TIME_BEFORE_UPSTREAM_CHECKS + 1)}}
        _status, _, where, _, _ = do_search_on_youtube(search_params, FakeYoutubeDynamodb)
        assert where == 'dynamodb'


def test_after_if_no_stream_ttl_expires_uses_youtube(search_params):
    with mock.patch.object(FakeYoutubeDynamodb, 'get_from_dynamodb') as get_from_dynamodb_mock:
        get_from_dynamodb_mock.return_value = {
            'result': {'S': json.dumps(FakeYoutubeDynamodb.request_from_youtube_offline(1, None)[1])},
            'time': {'N': str(time.time() - DYNAMODB_IF_NO_STREAM_TTL - 1)},
            'last_checked_time': {'N': str(time.time() - MIN_TIME_BEFORE_UPSTREAM_CHECKS - MAX_GRACE_PERIOD - 1)}}

        _status, _, where, _, _ = do_search_on_youtube(search_params, FakeYoutubeDynamodb)
        assert where == 'youtube'


def test_from_dynamodb_before_if_no_stream_ttl(search_params):
    with mock.patch.object(FakeYoutubeDynamodb, 'get_from_dynamodb') as get_from_dynamodb_mock:
        get_from_dynamodb_mock.return_value = {
            'result': {'S': json.dumps(FakeYoutubeDynamodb.request_from_youtube_offline(1, None)[1])},
            'time': {'N': str(time.time() - DYNAMODB_IF_STREAM_TTL + 1)},
            'last_checked_time': {'N': str(time.time() - MIN_TIME_BEFORE_UPSTREAM_CHECKS + 1)}}
        status, _, where, _, _ = do_search_on_youtube(search_params, FakeYoutubeDynamodb)
        assert where == 'dynamodb'


def test_force_video_id_writes_to_dynamodb(search_params):
    with mock.patch.object(FakeYoutubeDynamodb, 'write_to_dynamodb') as write_to_dynamodb_mock:
        force_video_id('ABCD', list(CHANNELS.keys())[0], 3600, youtube_and_dynamodb=FakeYoutubeDynamodb)
        assert write_to_dynamodb_mock.called
        assert write_to_dynamodb_mock.call_args[0][0] == list(CHANNELS.values())[0]
        assert write_to_dynamodb_mock.call_args[0][1]["items"][0]["id"]["videoId"]
        assert abs(int(write_to_dynamodb_mock.call_args[0][2] - time.time()) - 3600) <= 1


def test_empty_force_video_id_writes_to_dynamodb(search_params):
    with mock.patch.object(FakeYoutubeDynamodb, 'write_to_dynamodb') as write_to_dynamodb_mock:
        force_video_id('', list(CHANNELS.keys())[0], 3600, youtube_and_dynamodb=FakeYoutubeDynamodb)
        assert write_to_dynamodb_mock.called
        assert write_to_dynamodb_mock.call_args[0][0] == list(CHANNELS.values())[0]
        assert write_to_dynamodb_mock.call_args[0][1]["items"] == []
        assert abs(int(write_to_dynamodb_mock.call_args[0][2] - time.time()) - 3600) <= 1
