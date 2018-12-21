package pl.allegro.tech.discovery.consul.recipes.internal.http;

import okhttp3.Response;

import java.io.IOException;

public class BodyParser {

    public static String readBodyOrFallback(Response response, String fallback) {
        try {
            return response.body().string();
        } catch (IOException e) {
            return fallback;
        }
    }
}
