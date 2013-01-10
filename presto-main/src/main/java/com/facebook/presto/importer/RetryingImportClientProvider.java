package com.facebook.presto.importer;

import com.facebook.presto.spi.ImportClient;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.reflect.Reflection;

import javax.inject.Provider;
import java.lang.reflect.Method;

public class RetryingImportClientProvider
{
    public ImportClient get(final Provider<ImportClient> provider)
    {
        return Reflection.newProxy(ImportClient.class, new AbstractInvocationHandler()
        {
            @Override
            protected Object handleInvocation(Object proxy, Method method, Object[] args)
                    throws Throwable
            {
                // TODO: wrap with retry
                return method.invoke(provider.get(), args);
            }
        });
    }
}
