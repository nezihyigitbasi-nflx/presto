package com.facebook.presto.argus;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.presto.argus.peregrine.PeregrineClient;
import com.facebook.swift.prism.PrismNamespace;
import com.facebook.swift.prism.PrismNamespaceNotFound;
import com.facebook.swift.prism.PrismRepositoryError;
import com.facebook.swift.prism.PrismServiceClient;
import com.facebook.swift.prism.PrismServiceClientConfig;
import com.facebook.swift.prism.PrismServiceClientProvider;
import com.facebook.swift.service.ThriftClient;
import com.facebook.swift.service.ThriftClientConfig;
import com.facebook.swift.service.ThriftClientManager;
import com.facebook.swift.smc.Service;
import com.facebook.swift.smc.ServiceException;
import com.facebook.swift.smc.ServiceState;
import com.facebook.swift.smc.SmcClient;
import com.facebook.swift.smc.SmcClientConfig;
import com.facebook.swift.smc.SmcClientProvider;
import com.facebook.swift.smc.SmcUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import org.apache.thrift.TException;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.facebook.swift.service.ThriftClientManager.DEFAULT_NAME;

public class PeregrineClientFactory
        implements Closeable
{
    private final ThriftClientManager clientManager;
    private final SmcClientProvider smcClientProvider;
    private final PrismServiceClientProvider prismServiceClientProvider;
    private final ThriftClient<PeregrineClient> peregrineThriftClient;

    public PeregrineClientFactory(ThriftClientConfig config)
    {
        this.clientManager = new ThriftClientManager();

        this.smcClientProvider = new SmcClientProvider(
                new SmcClientConfig(),
                new ThriftClient<>(clientManager, SmcClient.class, config, DEFAULT_NAME));

        this.prismServiceClientProvider = new PrismServiceClientProvider(
                new PrismServiceClientConfig(),
                new ThriftClient<>(clientManager, PrismServiceClient.class, config, DEFAULT_NAME),
                smcClientProvider,
                clientManager);

        this.peregrineThriftClient = new ThriftClient<>(clientManager, PeregrineClient.class, config, DEFAULT_NAME);
    }

    public PrismNamespace lookupNamespace(String namespace)
    {
        try (PrismServiceClient client = prismServiceClientProvider.get()) {
            return client.getNamespace(namespace);
        }
        catch (PrismNamespaceNotFound e) {
            throw new RuntimeException("Namespace not found:" + namespace, e);
        }
        catch (PrismRepositoryError e) {
            throw new RuntimeException(e);
        }
    }

    public PeregrineClient create(String peregrineSmcTier)
    {
        List<HostAndPort> services = lookupSmcServices(peregrineSmcTier);
        if (services.isEmpty()) {
            throw new RuntimeException("No peregrine servers available: " + peregrineSmcTier);
        }

        Throwable lastException = null;
        for (HostAndPort service : shuffle(services)) {
            try {
                return peregrineThriftClient.open(new FramedClientConnector(service)).get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while connecting to Peregrine gateway", e);
            }
            catch (ExecutionException e) {
                lastException = e.getCause();
            }
        }
        throw new RuntimeException("Unable to connect to any Peregrine gateway: " + peregrineSmcTier, lastException);
    }

    @Override
    public void close()
    {
        clientManager.close();
    }

    private List<HostAndPort> lookupSmcServices(String tierName)
    {
        try (SmcClient client = smcClientProvider.get()) {
            Set<Service> services = SmcUtils.getAllServicesInTier(client, tierName);
            ImmutableList.Builder<HostAndPort> builder = ImmutableList.builder();
            for (Service service : services) {
                if (service.getState().contains(ServiceState.ENABLED)) {
                    builder.add(HostAndPort.fromParts(service.getIpAddress(), service.getPort()));
                }
            }
            return builder.build();
        }
        catch (ServiceException | TException e) {
            throw new RuntimeException("Unable to lookup SMC service: " + tierName, e);
        }
    }

    private static <T> List<T> shuffle(Iterable<T> iterable)
    {
        List<T> list = Lists.newArrayList(iterable);
        Collections.shuffle(list);
        return list;
    }
}
