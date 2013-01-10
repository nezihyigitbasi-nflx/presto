package com.facebook.presto.split;

import com.facebook.presto.hive.HiveClient;
import com.facebook.presto.importer.RetryingImportClientProvider;
import com.facebook.presto.spi.ImportClient;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.util.IterableUtils.shuffle;
import static com.google.common.base.Preconditions.checkArgument;

public class ImportClientFactory
{
    private final ServiceSelector selector;
    private final RetryingImportClientProvider provider;

    @Inject
    public ImportClientFactory(@ServiceType("hive-metastore") ServiceSelector selector, RetryingImportClientProvider provider)
    {
        this.selector = selector;
        this.provider = provider;
    }

    // TODO: includes hack to support presto installations supporting multiple hive dbs
    public ImportClient getClient(String sourceName)
    {
        checkArgument(sourceName.startsWith("hive_"), "bad source name: %s", sourceName);

        final String metastoreName = sourceName.split("_", 2)[1];
        checkArgument(!metastoreName.isEmpty(), "bad metastore name: %s", metastoreName);

        return provider.get(new Provider<ImportClient>() {
            @Override
            public ImportClient get()
            {
                return createClient(metastoreName);
            }
        });
    }

    private HiveClient createClient(String metastoreName)
    {
        List<HostAndPort> metastores = new ArrayList<>();
        for (ServiceDescriptor descriptor : selector.selectAllServices()) {
            String thrift = descriptor.getProperties().get("thrift");
            String name = descriptor.getProperties().get("name");
            if ((thrift != null) && metastoreName.equals(name)) {
                try {
                    HostAndPort metastore = HostAndPort.fromString(thrift);
                    checkArgument(metastore.hasPort());
                    metastores.add(metastore);
                }
                catch (IllegalArgumentException ignored) {
                }
            }
        }

        if (metastores.isEmpty()) {
            throw new RuntimeException(String.format("hive metastore not available for name %s in pool %s", metastoreName, selector.getPool()));
        }

        HostAndPort metastore = shuffle(metastores).get(0);
        return new HiveClient(metastore.getHostText(), metastore.getPort());
    }
}
