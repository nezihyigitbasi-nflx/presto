package com.facebook.presto.server;

import com.facebook.presto.Session;
import com.facebook.presto.client.ParallelStatus;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.security.AccessControl;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_PARALLEL_COUNT;
import static com.facebook.presto.server.ResourceUtil.assertRequest;
import static com.facebook.presto.server.ResourceUtil.badRequest;
import static com.facebook.presto.server.ResourceUtil.createSessionForRequest;
import static com.facebook.presto.server.ResourceUtil.trimEmptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/v1/parallel")
public class ParallelResource
{
    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();
    private static final long DESIRED_RESULT_BYTES = new DataSize(1, MEGABYTE).toBytes();

    private final QueryManager queryManager;
    private final AccessControl accessControl;
    private final SessionPropertyManager sessionPropertyManager;
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final QueryIdGenerator queryIdGenerator;

    private final ConcurrentMap<QueryId, ParallelQuery> queries = new ConcurrentHashMap<>();

    @Inject
    public ParallelResource(
            QueryManager queryManager,
            AccessControl accessControl,
            SessionPropertyManager sessionPropertyManager,
            ExchangeClientSupplier exchangeClientSupplier,
            QueryIdGenerator queryIdGenerator)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
    }

    @POST
    @Produces(APPLICATION_JSON)
    public Response createQuery(
            String statement,
            @Context HttpServletRequest request,
            @Context UriInfo uriInfo)
            throws InterruptedException
    {
        assertRequest(!isNullOrEmpty(statement), "SQL statement is empty");
        int parallelCount = getParallelCount(request);

        QueryId queryId = queryIdGenerator.createNextQueryId();
        Session session = createSessionForRequest(request, accessControl, sessionPropertyManager, queryId);

        ParallelQuery query = new ParallelQuery(session, statement, parallelCount);

        queries.put(queryId, query);

        ImmutableList.Builder<URI> dataUris = ImmutableList.builder();
        for (int i = 0; i < parallelCount; i++) {
            dataUris.add(uriInfo.getRequestUriBuilder()
                    .path(queryId.toString())
                    .path(String.valueOf(i))
                    .replaceQuery("")
                    .build());
        }

        URI nextUri = uriInfo.getRequestUriBuilder()
                .path(queryId.toString())
                .replaceQuery("")
                .build();

        ParallelStatus status = new ParallelStatus(queryId.toString(), null, null, nextUri, dataUris.build(), null, null);

        return Response.ok(status).build();
    }

    @GET
    @Path("{queryId}")
    @Produces(APPLICATION_JSON)
    public Response getStatus(
            @PathParam("queryId") QueryId queryId,
            @QueryParam("maxWait") Duration maxWait,
            @Context UriInfo uriInfo)
            throws InterruptedException
    {
        ParallelQuery query = queries.get(queryId);
        if (query == null) {
            return Response.status(NOT_FOUND).build();
        }

        queryManager

        Duration wait = WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait);
        return getQueryResults(query, Optional.of(token), uriInfo, wait);
    }

    @GET
    @Path("{queryId}/{output}")
    @Produces(APPLICATION_JSON)
    public Response getOutput(
            @PathParam("queryId") QueryId queryId,
            @PathParam("output") int output,
            @QueryParam("maxWait") Duration maxWait,
            @Context UriInfo uriInfo)
            throws InterruptedException
    {
        ParallelQuery query = queries.get(queryId);
        if (query == null) {
            return Response.status(NOT_FOUND).build();
        }

        Duration wait = WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait);
        return getQueryResults(query, Optional.of(token), uriInfo, wait);
    }

    private static int getParallelCount(HttpServletRequest request)
    {
        String countHeader = trimEmptyToNull(request.getHeader(PRESTO_PARALLEL_COUNT));
        assertRequest(countHeader != null, "%s header must be set");
        try {
            int count = Integer.parseInt(countHeader);
            assertRequest(count > 0, "Parallel count must be > 0");
            return count;
        }
        catch (NumberFormatException e) {
            throw badRequest(PRESTO_PARALLEL_COUNT + " header is invalid");
        }

    }

    private static class ParallelQuery
    {
        private final Session session;
        private final String statement;
        private final int parallelCount;

        private ParallelQuery(Session session, String statement, int parallelCount)
        {
            this.session = requireNonNull(session, "session is null");
            this.statement = requireNonNull(statement, "statement is null");
            this.parallelCount = parallelCount;
        }
    }
}
