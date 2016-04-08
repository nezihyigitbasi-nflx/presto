/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server.testing;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

public class NamespacingMBeanServer
        extends ForwardingMBeanServer
{
    private static final AtomicInteger NEXT_ID = new AtomicInteger();

    private final String prefix = format("test%03d", NEXT_ID.getAndIncrement());

    public NamespacingMBeanServer(MBeanServer delegate)
    {
        super(delegate);
    }

    @Override
    public ObjectInstance registerMBean(Object object, ObjectName name)
            throws MBeanRegistrationException, NotCompliantMBeanException, InstanceAlreadyExistsException
    {
        ObjectName prefixed;
        try {
            String domain = prefix + "." + name.getDomain();
            prefixed = new ObjectName(domain, name.getKeyPropertyList());
        }
        catch (MalformedObjectNameException e) {
            throw new MBeanRegistrationException(e);
        }
        return super.registerMBean(object, prefixed);
    }
}
