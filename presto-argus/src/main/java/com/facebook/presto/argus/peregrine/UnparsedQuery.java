package com.facebook.presto.argus.peregrine;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

@ThriftStruct
public class UnparsedQuery
{
    private final String username;
    private final String request;
    private final List<String> tags;
    private final String database;

    @ThriftConstructor
    public UnparsedQuery(String username, String request, List<String> tags, String database)
    {
        this.username = checkNotNull(username, "username is null");
        this.request = checkNotNull(request, "request is null");
        this.tags = ImmutableList.copyOf(checkNotNull(tags, "tags is null"));
        this.database = checkNotNull(database, "database is null");
    }

    @ThriftField(1)
    public String getUsername()
    {
        return username;
    }

    @ThriftField(2)
    public String getRequest()
    {
        return request;
    }

    @ThriftField(3)
    public List<String> getTags()
    {
        return tags;
    }

    @ThriftField(4)
    public String getDatabase()
    {
        return database;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("username", username)
                .add("request", request)
                .add("tags", tags)
                .add("database", database)
                .toString();
    }
}
