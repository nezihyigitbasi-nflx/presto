package com.facebook.presto.argus.peregrine;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class QueryIdNotFoundException
        extends Exception
{
    @ThriftConstructor
    public QueryIdNotFoundException() {}
}
