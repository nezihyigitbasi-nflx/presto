package com.facebook.presto.argus.peregrine;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

@ThriftStruct
public class QueryResponse
{
    private final QueryStatus status;
    private final QueryResult result;

    @ThriftConstructor
    public QueryResponse(QueryStatus status, QueryResult result)
    {
        this.status = checkNotNull(status, "status is null");
        this.result = checkNotNull(result, "result is null");
    }

    @ThriftField(1)
    public QueryStatus getStatus()
    {
        return status;
    }

    @ThriftField(2)
    public QueryResult getResult()
    {
        return result;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("status", status)
                .add("result", result)
                .toString();
    }
}
