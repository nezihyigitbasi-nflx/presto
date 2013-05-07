package com.facebook.presto.argus;

import com.facebook.presto.argus.peregrine.PeregrineClient;
import com.facebook.presto.argus.peregrine.PeregrineErrorCode;
import com.facebook.presto.argus.peregrine.PeregrineException;
import com.facebook.presto.argus.peregrine.QueryId;
import com.facebook.presto.argus.peregrine.QueryIdNotFoundException;
import com.facebook.presto.argus.peregrine.QueryResponse;
import com.facebook.presto.argus.peregrine.QueryResult;
import com.facebook.presto.argus.peregrine.QueryStatus;
import com.facebook.presto.argus.peregrine.UnparsedQuery;
import com.facebook.swift.prism.PrismNamespace;
import com.facebook.swift.service.ThriftClientConfig;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.units.Duration;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.repeat;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.nanoTime;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PeregrineRunner
        implements Closeable
{
    private final Duration timeLimit;
    private final PeregrineClientFactory clientFactory;
    private final ExecutorService timeLimiterExecutor;
    private final TimeLimiter timeLimiter;

    public PeregrineRunner(Duration timeLimit)
    {
        this.timeLimit = checkNotNull(timeLimit, "timeLimit is null");
        ThriftClientConfig config = new ThriftClientConfig()
                .setSocksProxy(HostAndPort.fromString("localhost:1080"))
                .setReadTimeout(new Duration(10, SECONDS))
                .setWriteTimeout(new Duration(10, SECONDS));
        this.clientFactory = new PeregrineClientFactory(config);
        this.timeLimiterExecutor = Executors.newCachedThreadPool();
        this.timeLimiter = new SimpleTimeLimiter(timeLimiterExecutor);
    }

    @Override
    public void close()
    {
        clientFactory.close();
        timeLimiterExecutor.shutdownNow();
    }

    public List<List<Object>> execute(String username, String namespace, String sql)
            throws PeregrineException
    {
        PrismNamespace ns = clientFactory.lookupNamespace(namespace);
        try (PeregrineClient client = createClient(ns)) {
            System.out.printf("Peregrine: starting...\r");
            QueryId queryId = client.submitQuery(new UnparsedQuery(
                    username,
                    "WITH true AS mode.exact " + sql,
                    ImmutableList.<String>of(),
                    ns.getHiveDatabaseName()));

            long start = nanoTime();
            QueryResponse response;
            do {
                if (nanosSince(start).compareTo(timeLimit) > 0) {
                    throw new UncheckedTimeoutException();
                }
                try {
                    response = client.getResponse(queryId);
                    QueryStatus status = response.getStatus();
                    System.out.printf("Peregrine: %s: [%s / %s] [%s] [%s]\r",
                            status.getState(),
                            status.getCompletedSize(),
                            status.getTotalSize(),
                            status.getQueueRank(),
                            currentTimeMillis() / 1000).flush();
                }
                catch (QueryIdNotFoundException e) {
                    throw new PeregrineException("query ID not found", PeregrineErrorCode.UNKNOWN);
                }
            }
            while (!response.getStatus().getState().isDone());

            return peregrineResults(response.getResult());
        }
        finally {
            System.out.printf("%s\r", repeat(" ", 70)).flush();
        }
    }

    private PeregrineClient createClient(PrismNamespace ns)
    {
        PeregrineClient client = clientFactory.create(ns.getPeregrineGateway());
        return timeLimiter.newProxy(client, PeregrineClient.class, (long) timeLimit.toMillis(), MILLISECONDS);
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
                        return s.equals("null") ? null : s;
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
                        if (s.equals("null")) {
                            return null;
                        }
                        s = s.trim();
                        switch (s.toLowerCase()) {
                            case "nan":
                                return NaN;
                            case "infinity":
                                return POSITIVE_INFINITY;
                            case "-infinity":
                                return NEGATIVE_INFINITY;
                        }
                        return parseDouble(s);
                    }
                };
        }
        throw new IllegalArgumentException("unsupported Peregrine type: " + type);
    }
}
