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
import com.facebook.swift.prism.PrismNamespaceNotFound;
import com.facebook.swift.service.RuntimeTTransportException;
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

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
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

    public Results execute(String username, String namespace, String sql)
            throws PeregrineException
    {
        PrismNamespace ns;
        try {
            ns = clientFactory.lookupNamespace(namespace);
        }
        catch (PrismNamespaceNotFound e) {
            throw new PeregrineException("prism namespace not found: " + namespace, PeregrineErrorCode.UNKNOWN);
        }

        try (PeregrineClient client = createClient(ns)) {
            QueryId queryId;
            try {
                queryId = client.submitQuery(new UnparsedQuery(
                        username,
                        "WITH true AS mode.exact " + sql,
                        ImmutableList.<String>of(),
                        ns.getHiveDatabaseName()));
            }
            catch (RuntimeTTransportException e) {
                throw new PeregrineException("failed to submit query", PeregrineErrorCode.UNKNOWN);
            }

            long start = nanoTime();
            QueryResponse response;
            do {
                if (nanosSince(start).compareTo(timeLimit) > 0) {
                    throw new UncheckedTimeoutException();
                }
                try {
                    response = client.getResponse(queryId);
                }
                catch (QueryIdNotFoundException e) {
                    throw new PeregrineException("query ID not found", PeregrineErrorCode.UNKNOWN);
                }
                catch (RuntimeTTransportException e) {
                    throw new UncheckedTimeoutException(e);
                }
            }
            while (!response.getStatus().getState().isDone());

            return peregrineResults(response.getResult());
        }
    }

    private PeregrineClient createClient(PrismNamespace ns)
    {
        PeregrineClient client = clientFactory.create(ns.getPeregrineGateway());
        return timeLimiter.newProxy(client, PeregrineClient.class, (long) timeLimit.toMillis(), MILLISECONDS);
    }

    private static Results peregrineResults(QueryResult result)
            throws PeregrineException
    {
        List<Function<String, Object>> types = new ArrayList<>();
        for (String type : result.getTypes()) {
            types.add(typeConversionFunction(type));
        }

        Splitter splitter = Splitter.on((char) 1);
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();

        for (String line : result.getValues()) {
            List<String> values = ImmutableList.copyOf(splitter.split(line).iterator());
            if (values.size() != types.size()) {
                String error = format("wrong column count (expected %s, was %s)", types.size(), values.size());
                throw new PeregrineException(error, PeregrineErrorCode.UNKNOWN);
            }

            List<Object> row = new ArrayList<>();
            for (int i = 0; i < types.size(); i++) {
                row.add(types.get(i).apply(values.get(i)));
            }
            rows.add(unmodifiableList(row));
        }

        return new Results(result.getHeaders(), rows.build());
    }

    private static Function<String, Object> typeConversionFunction(String type)
            throws PeregrineException
    {
        switch (type) {
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
            default:
                return new Function<String, Object>()
                {
                    @Override
                    public Object apply(String s)
                    {
                        return s.equals("null") ? null : s;
                    }
                };
        }
    }

    public static class Results
    {
        private final List<String> columns;
        private final List<List<Object>> rows;

        public Results(List<String> columns, List<List<Object>> rows)
        {
            this.columns = checkNotNull(columns, "columns is null");
            this.rows = checkNotNull(rows, "rows is null");
        }

        public List<String> getColumns()
        {
            return columns;
        }

        public List<List<Object>> getRows()
        {
            return rows;
        }
    }
}
