package com.facebook.presto.argus.peregrine;

import com.facebook.swift.codec.ThriftEnumValue;

public enum QueryState
{
    WAITING(1),
    SCHEDULED(2),
    RUNNING(3),
    SUCCEEDED(4),
    FAILED(5);

    private final int value;

    QueryState(int value)
    {
        this.value = value;
    }

    @ThriftEnumValue
    public int getValue()
    {
        return value;
    }

    public boolean isDone()
    {
        return (this == SUCCEEDED) || (this == FAILED);
    }
}
