package com.facebook.presto.argus.peregrine;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class PeregrineException
        extends Exception
{
    private final PeregrineErrorCode code;

    @ThriftConstructor
    public PeregrineException(String message, PeregrineErrorCode code)
    {
        super(message);
        this.code = code;
    }

    @Override
    @ThriftField(1)
    public String getMessage()
    {
        return super.getMessage();
    }

    @ThriftField(2)
    public PeregrineErrorCode getCode()
    {
        return code;
    }

    @Override
    public String toString()
    {
        return super.toString() + " (" + code + ")";
    }
}
