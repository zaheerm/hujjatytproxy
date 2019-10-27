function wp_hujjat_live_stream_log(message) {
    console.log("[wp_hujjat_live_stream] " + message);
}

function wp_hujjat_live_stream_put_offline_info(channel) {
    wp_hujjat_live_stream_log(channel + 'live stream is offline');
    jQuery("div#wp_hujjat_live_stream_" + channel).removeClass('wp-hujjat-live-stream-online');
    jQuery("div#wp_hujjat_live_stream_" + channel).addClass('wp-hujjat-live-stream-offline');
}

function wp_hujjat_live_stream_is_any_stream_online(channels) {
    var params = {};
    url = "https://api.poc.hujjat.org/youtube/live/";
    let offline_channels = [];
    jQuery.getJSON(url, function(data) {
        wp_hujjat_live_stream_log('going through the channels');

        for(var key in data) {
            if (jQuery.inArray(key, channels) != -1) {
                if (data[key]["status_code"] >= 200 || data[key]["status_code"] <= 299) {
                    wp_hujjat_live_stream_log("got 2xx for " + key);
                    if (data[key]["result"]["items"].length > 0) {
                        let videoid = data[key]["result"]["items"][0]["id"]["videoId"];
                        wp_hujjat_live_stream_log(key + ' live stream is online');
                        jQuery("div#wp_hujjat_live_stream_" + key).removeClass('wp-hujjat-live-stream-offline');
                        jQuery("div#wp_hujjat_live_stream_" + key).addClass('wp-hujjat-live-stream-online');
                    }
                    else {
                        wp_hujjat_live_stream_put_offline_info(key);
                        offline_channels.push(key);
                    }
                }
            }
        }
        setTimeout(function() { wp_hujjat_live_stream_is_any_stream_online(offline_channels);}, 60000);
    }).fail(function() {
        wp_hujjat_live_stream_log("failure querying youtube api");
        setTimeout(function() { wp_hujjat_live_stream_is_any_stream_online(offline_channels);}, 60000);
    });
}

jQuery(document).ready(function($) {
    /* live stream widget hiding */
    wp_hujjat_live_stream_is_any_stream_online(["mainhall", "elc", "ladies"]);
});