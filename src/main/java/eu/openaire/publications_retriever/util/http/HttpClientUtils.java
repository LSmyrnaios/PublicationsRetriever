package eu.openaire.publications_retriever.util.http;

import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.http.HttpClient;
import java.time.Duration;

public class HttpClientUtils {

    private static final HttpClient client;

    public static final CookieManager cookieManager = new java.net.CookieManager();

    private static final Duration maxConnectWaitingTime = Duration.ofSeconds(5);  // Max time to wait for a hand-shake with the server.

    static {
        cookieManager.setCookiePolicy(CookiePolicy.ACCEPT_ORIGINAL_SERVER);
        CookieHandler.setDefault(cookieManager);

        client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NEVER) // We handle the redirection ourselves, to impose limits and optimizations.
                .connectTimeout(maxConnectWaitingTime)
                .cookieHandler(CookieHandler.getDefault())
                .build();
    }

    public static HttpClient getHttpClient() {
        return client;
    }
}
