package com.facebook.presto.argus.peregrine;

import com.facebook.swift.service.ThriftException;
import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;

@ThriftService("Peregrine")
public interface PeregrineClient
        extends AutoCloseable
{
    @Override
    void close();

    @ThriftMethod
    QueryId submitQuery(UnparsedQuery query)
            throws PeregrineException;

    @ThriftMethod(exception = {
            @ThriftException(id = 1, type = PeregrineException.class),
            @ThriftException(id = 2, type = QueryIdNotFoundException.class)})
    QueryResponse getResponse(QueryId id)
            throws PeregrineException, QueryIdNotFoundException;
}
