package com.facebook.presto.argus.peregrine;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

@ThriftStruct
public class QueryResult
{
    private final List<String> headers;
    private final List<String> types;
    private final List<String> values;
    private final boolean exact;
    private final int failures;
    private final boolean tracing;

    @ThriftConstructor
    public QueryResult(List<String> headers, List<String> types, List<String> values, boolean exact, int failures, boolean tracing)
    {
        this.headers = ImmutableList.copyOf(checkNotNull(headers, "headers is null"));
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        this.values = ImmutableList.copyOf(checkNotNull(values, "values is null"));
        this.exact = exact;
        this.failures = failures;
        this.tracing = tracing;
    }

    @ThriftField(1)
    public List<String> getHeaders()
    {
        return headers;
    }

    @ThriftField(2)
    public List<String> getTypes()
    {
        return types;
    }

    @ThriftField(3)
    public List<String> getValues()
    {
        return values;
    }

    @ThriftField(4)
    public boolean isExact()
    {
        return exact;
    }

    @ThriftField(5)
    public int getFailures()
    {
        return failures;
    }

    @ThriftField(6)
    public boolean isTracing()
    {
        return tracing;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("headers", headers)
                .add("types", types)
                .add("values", values)
                .add("exact", exact)
                .add("failures", failures)
                .add("tracing", tracing)
                .toString();
    }
}
