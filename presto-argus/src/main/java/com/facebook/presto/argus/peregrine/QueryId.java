package com.facebook.presto.argus.peregrine;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import static com.google.common.base.Objects.toStringHelper;

@ThriftStruct
public class QueryId
{
    private final long run;
    private final long seqno;

    @ThriftConstructor
    public QueryId(long run, long seqno)
    {
        this.run = run;
        this.seqno = seqno;
    }

    @ThriftField(1)
    public long getRun()
    {
        return run;
    }

    @ThriftField(2)
    public long getSeqno()
    {
        return seqno;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("run", run)
                .add("seqno", seqno)
                .toString();
    }
}
