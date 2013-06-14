package com.facebook.presto.jdbc;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.json.JsonCodec.jsonCodec;

public class QueryExecutor
        implements Closeable
{
    private final JsonCodec<QueryResults> queryInfoCodec;
    private final HttpClient httpClient;

    private QueryExecutor(String userAgent, JsonCodec<QueryResults> queryResultsCodec)
    {
        checkNotNull(userAgent, "userAgent is null");
        checkNotNull(queryResultsCodec, "queryResultsCodec is null");

        HttpClientConfig config = new HttpClientConfig()
                .setConnectTimeout(new Duration(10, TimeUnit.SECONDS));
        Set<HttpRequestFilter> filters = ImmutableSet.<HttpRequestFilter>of(new UserAgentRequestFilter(userAgent));

        this.queryInfoCodec = queryResultsCodec;
        this.httpClient = new ApacheHttpClient(config, filters);
    }

    public StatementClient startQuery(ClientSession session, String query)
    {
        return new StatementClient(httpClient, queryInfoCodec, session, query);
    }

    @Override
    public void close()
    {
        httpClient.close();
    }

    public static QueryExecutor create(String userAgent)
    {
        return new QueryExecutor(userAgent, jsonCodec(QueryResults.class));
    }
}
