package com.facebook.presto.argus.peregrine;

import com.facebook.swift.codec.ThriftEnumValue;

public enum PeregrineErrorCode
{
    NONE(0),
    PARSING_ERROR(1),
    COMPILATION_ERROR(2),
    METASTORE_ERROR(3),
    NAMENODE_ERROR(4),
    OUT_OF_MEMORY(5),
    UNABLE_TO_OPEN_FILE(6),
    RECORD_READ_ERROR(7),
    NO_PROGRESS(8),
    LATE_RESPONSE(9),
    TIMED_OUT(10),
    USER_ERROR(11),
    HBASE_ERROR(12),
    SYSTEM_OVERLOAD(13),
    UNKNOWN(31);

    private final int value;

    PeregrineErrorCode(int value)
    {
        this.value = value;
    }

    @ThriftEnumValue
    public int getValue()
    {
        return value;
    }

    public boolean isInvalidQuery()
    {
        return (this == PARSING_ERROR) ||
                (this == COMPILATION_ERROR) ||
                (this == USER_ERROR);
    }
}
