import json
import os
from chalice import Chalice, Response
from cachetools import TTLCache
import requests

app = Chalice(app_name='hujjatytproxy')
CACHE = TTLCache(maxsize=10, ttl=120)
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


def do_search_on_youtube(params):
    channel = params.get("channelId")
    result = CACHE.get(channel)
    if CACHE.get(channel):
        return 200, result
    else:
        try:
            if not params.get("key"):
                params["key"] = os.environ["YOUTUBE_API_KEY"]
            r = requests.get("https://www.googleapis.com/youtube/v3/search", params=params)
            if r.status_code == requests.codes.ok:
                result = r.json()
                CACHE[channel] = result
            return r.status_code, result
        except Exception as exc:
            return 500, {}


@app.route('/v3/search', cors=True)
def youtube():
    status_code, result = do_search_on_youtube(app.current_request.query_params)
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
        status_code, result = do_search_on_youtube(params)
        if status_code == requests.codes.ok:
            if len(result.get("items", [])) > 0:
                any_live = True
        results[channel] = {
            "status_code": status_code,
            "result": result}
    results["any_live"] = any_live
    return results
