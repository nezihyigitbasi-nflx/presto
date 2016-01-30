package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class ParallelStatus
{
    private final String id;
    private final URI infoUri;
    private final URI partialCancelUri;
    private final URI nextUri;
    private final List<URI> dataUris;
    private final StatementStats stats;
    private final QueryError error;

    @JsonCreator
    public ParallelStatus(
            @JsonProperty("id") String id,
            @JsonProperty("infoUri") URI infoUri,
            @JsonProperty("partialCancelUri") URI partialCancelUri,
            @JsonProperty("nextUri") URI nextUri,
            @JsonProperty("dataUris") List<URI> dataUris,
            @JsonProperty("stats") StatementStats stats,
            @JsonProperty("error") QueryError error)
    {
        this.id = requireNonNull(id, "id is null");
        this.infoUri = infoUri;
        this.partialCancelUri = partialCancelUri;
        this.nextUri = nextUri;
        this.dataUris = requireNonNull(dataUris, "dataUris is null");
        this.stats = stats;
        this.error = error;
    }

    @JsonProperty
    public String getId()
    {
        return id;
    }

    @Nullable
    @JsonProperty
    public URI getInfoUri()
    {
        return infoUri;
    }

    @Nullable
    @JsonProperty
    public URI getPartialCancelUri()
    {
        return partialCancelUri;
    }

    @Nullable
    @JsonProperty
    public URI getNextUri()
    {
        return nextUri;
    }

    @JsonProperty
    public List<URI> getDataUris()
    {
        return dataUris;
    }

    @Nullable
    @JsonProperty
    public StatementStats getStats()
    {
        return stats;
    }

    @Nullable
    @JsonProperty
    public QueryError getError()
    {
        return error;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("infoUri", infoUri)
                .add("partialCancelUri", partialCancelUri)
                .add("nextUri", nextUri)
                .add("dataUris", dataUris)
                .add("stats", stats)
                .add("error", error)
                .omitNullValues()
                .toString();
    }
}
