package com.facebook.presto.argus.peregrine;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

@ThriftStruct
public class QueryStatus
{
    private final QueryState state;
    private final long completedSize;
    private final long scanSize;
    private final long scheduledSize;
    private final long totalSize;
    private final long startTime;
    private final long endTime;
    private final long rowCount;
    private final Error error;
    private final long queueRank;
    private final long slotsUsed;

    @ThriftConstructor
    public QueryStatus(
            QueryState state,
            long completedSize,
            long scanSize,
            long scheduledSize,
            long totalSize,
            long startTime,
            long endTime,
            long rowCount,
            Error error,
            long queueRank,
            long slotsUsed)
    {
        this.state = checkNotNull(state, "state is null");
        this.completedSize = completedSize;
        this.scanSize = scanSize;
        this.scheduledSize = scheduledSize;
        this.totalSize = totalSize;
        this.startTime = startTime;
        this.endTime = endTime;
        this.rowCount = rowCount;
        this.error = error;
        this.queueRank = queueRank;
        this.slotsUsed = slotsUsed;
    }

    @ThriftField(1)
    public QueryState getState()
    {
        return state;
    }

    @ThriftField(2)
    public long getCompletedSize()
    {
        return completedSize;
    }

    @ThriftField(3)
    public long getScanSize()
    {
        return scanSize;
    }

    @ThriftField(4)
    public long getScheduledSize()
    {
        return scheduledSize;
    }

    @ThriftField(5)
    public long getTotalSize()
    {
        return totalSize;
    }

    @ThriftField(6)
    public long getStartTime()
    {
        return startTime;
    }

    @ThriftField(7)
    public long getEndTime()
    {
        return endTime;
    }

    @ThriftField(8)
    public long getRowCount()
    {
        return rowCount;
    }

    @ThriftField(9)
    public Error getError()
    {
        return error;
    }

    @ThriftField(10)
    public long getQueueRank()
    {
        return queueRank;
    }

    @ThriftField(11)
    public long getSlotsUsed()
    {
        return slotsUsed;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("state", state)
                .add("completedSize", completedSize)
                .add("scanSize", scanSize)
                .add("scheduledSize", scheduledSize)
                .add("totalSize", totalSize)
                .add("startTime", startTime)
                .add("endTime", endTime)
                .add("rowCount", rowCount)
                .add("error", error)
                .add("queueRank", queueRank)
                .add("slotsUsed", slotsUsed)
                .toString();
    }
}
