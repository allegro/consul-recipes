package pl.allegro.tech.discovery.consul.recipes.watch;

import okhttp3.HttpUrl;

interface ReconnectCallback {

    void reconnect(HttpUrl endpoint, long index, ConsulLongPollCallback callback);

}
