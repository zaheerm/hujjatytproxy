from chalice import Chalice, Response
from cachetools import TTLCache
import requests

app = Chalice(app_name='hujjatytproxy')
CACHE = TTLCache(maxsize=10, ttl=60)


@app.route('/youtube/v3/search')
def youtube():
    channel = app.current_request.query_params.get("channelId")
    result = CACHE.get(channel)
    if not CACHE.get(channel):
        try:
            r = requests.get("https://www.googleapis.com/youtube/v3/search", params=app.current_request.query_params)
            if r.status_code == requests.codes.ok:
                result = r.json()
                CACHE[channel] = result
            else:
                return Response(
                    body=r.text, status_code=r.status_code, headers={"Content-Type": r.headers["Content-Type"]})
        except Exception as exc:
            print(exc)

    return result
