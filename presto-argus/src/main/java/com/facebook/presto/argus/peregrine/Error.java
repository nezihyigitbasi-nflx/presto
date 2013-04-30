package com.facebook.presto.argus.peregrine;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

@ThriftStruct
public class Error
{
    private final PeregrineErrorCode code;
    private final String message;

    @ThriftConstructor
    public Error(PeregrineErrorCode code, String message)
    {
        this.code = checkNotNull(code, "code is null");
        this.message = checkNotNull(message, "message is null");
    }

    @ThriftField(1)
    public PeregrineErrorCode getCode()
    {
        return code;
    }

    @ThriftField(2)
    public String getMessage()
    {
        return message;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("code", code)
                .add("message", message)
                .toString();
    }
}
