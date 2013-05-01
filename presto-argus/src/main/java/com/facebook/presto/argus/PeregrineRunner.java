package com.facebook.presto.argus;

import com.facebook.presto.argus.peregrine.PeregrineClient;
import com.facebook.presto.argus.peregrine.PeregrineErrorCode;
import com.facebook.presto.argus.peregrine.PeregrineException;
import com.facebook.presto.argus.peregrine.QueryId;
import com.facebook.presto.argus.peregrine.QueryIdNotFoundException;
import com.facebook.presto.argus.peregrine.QueryResponse;
import com.facebook.presto.argus.peregrine.QueryResult;
import com.facebook.presto.argus.peregrine.UnparsedQuery;
import com.facebook.swift.prism.PrismNamespace;
import com.facebook.swift.service.ThriftClientConfig;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PeregrineRunner
        implements Closeable
{
    private final PeregrineClientFactory clientFactory;

    public PeregrineRunner()
    {
        ThriftClientConfig config = new ThriftClientConfig()
                .setSocksProxy(HostAndPort.fromString("localhost:1080"))
                .setReadTimeout(new Duration(10, SECONDS))
                .setWriteTimeout(new Duration(10, SECONDS));
        this.clientFactory = new PeregrineClientFactory(config);
    }

    @Override
    public void close()
    {
        clientFactory.close();
    }

    public List<List<Object>> execute(String username, String namespace, String sql)
            throws PeregrineException
    {
        PrismNamespace ns = clientFactory.lookupNamespace(namespace);
        try (PeregrineClient client = clientFactory.create(ns.getPeregrineGateway())) {
            QueryId queryId = client.submitQuery(new UnparsedQuery(
                    username,
                    "WITH true AS mode.exact " + sql,
                    ImmutableList.<String>of(),
                    ns.getHiveDatabaseName()));

            QueryResponse response;
            do {
                try {
                    response = client.getResponse(queryId);
                }
                catch (QueryIdNotFoundException e) {
                    throw new PeregrineException("query ID not found", PeregrineErrorCode.UNKNOWN);
                }
            }
            while (!response.getStatus().getState().isDone());

            return peregrineResults(response.getResult());
        }
    }

    private static List<List<Object>> peregrineResults(QueryResult result)
    {
        List<Function<String, Object>> types = new ArrayList<>();
        for (String type : result.getTypes()) {
            types.add(typeConversionFunction(type));
        }

        Splitter splitter = Splitter.on((char) 1);
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();

        for (String line : result.getValues()) {
            List<String> values = ImmutableList.copyOf(splitter.split(line).iterator());
            checkArgument(values.size() == types.size(), "wrong column count (expected %s, was %s)", types.size(), values.size());

            List<Object> row = new ArrayList<>();
            for (int i = 0; i < types.size(); i++) {
                row.add(types.get(i).apply(values.get(i)));
            }
            rows.add(unmodifiableList(row));
        }

        return rows.build();
    }

    private static Function<String, Object> typeConversionFunction(String type)
    {
        switch (type) {
            case "string":
                return new Function<String, Object>()
                {
                    @Override
                    public Object apply(String s)
                    {
                        return s;
                    }
                };
            case "bigint":
                return new Function<String, Object>()
                {
                    @Override
                    public Object apply(String s)
                    {
                        return s.equals("null") ? null : parseLong(s);
                    }
                };
            case "double":
                return new Function<String, Object>()
                {
                    @Override
                    public Object apply(String s)
                    {
                        return s.equals("null") ? null : parseDouble(s);
                    }
                };
        }
        throw new IllegalArgumentException("unsupported Peregrine type: " + type);
    }
}
