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

import com.facebook.presto.spi.ColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestJdbcOutputTableHandle
{
    private final JdbcOutputTableHandle tableHandle = new JdbcOutputTableHandle(
            "connectorId",
            "catalog",
            "schema",
            "table",
            ImmutableList.of("abc", "xyz"),
            ImmutableList.of(ColumnType.STRING, ColumnType.STRING),
            "test",
            "tmp_table",
            "jdbc:junk",
            ImmutableMap.of("user", "test"));

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<JdbcOutputTableHandle> codec = jsonCodec(JdbcOutputTableHandle.class);
        String json = codec.toJson(tableHandle);
        JdbcOutputTableHandle copy = codec.fromJson(json);
        assertEquals(copy, tableHandle);
    }
}
