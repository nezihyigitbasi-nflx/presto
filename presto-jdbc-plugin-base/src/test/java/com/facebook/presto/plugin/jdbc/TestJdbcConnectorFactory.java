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
package com.facebook.presto.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.testng.annotations.Test;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

public class TestJdbcConnectorFactory
{
    @Test
    public void test()
            throws Exception
    {
        JdbcConnectorFactory connectorFactory = new JdbcConnectorFactory(
                "test",
                new TestingH2JdbcModule(),
                ImmutableMap.<String, String>of(),
                getClass().getClassLoader());

        connectorFactory.create("test", ImmutableMap.of(
                "driver-class", "org.h2.Driver",
                "connection-url", "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1"
        ));
    }

    private static class TestingH2JdbcModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            bindConfig(binder).to(BaseJdbcConfig.class);
        }

        @Provides
        public JdbcClient provideJdbcClient(JdbcConnectorId id, BaseJdbcConfig config)
        {
            return new BaseJdbcClient(id, config, "\"");
        }
    }
}
