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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.plugin.jdbc.TestingJdbcClient.createTestingJdbcClient;
import static com.facebook.presto.spi.ColumnType.LONG;
import static com.facebook.presto.spi.ColumnType.STRING;
import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestJdbcRecordSet
{
    private JdbcClient jdbcClient;
    private JdbcSplit split;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        jdbcClient = createTestingJdbcClient("test" + System.nanoTime());
        split = TestingJdbcClient.getSplit(jdbcClient, "example", "numbers");
    }

    @Test
    public void testGetColumnTypes()
            throws Exception
    {
        RecordSet recordSet = new JdbcRecordSet(jdbcClient, split, ImmutableList.of(
                new JdbcColumnHandle("test", "text", STRING, 0),
                new JdbcColumnHandle("test", "value", LONG, 1)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(STRING, LONG));

        recordSet = new JdbcRecordSet(jdbcClient, split, ImmutableList.of(
                new JdbcColumnHandle("test", "value", LONG, 1),
                new JdbcColumnHandle("test", "text", STRING, 0)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(LONG, STRING));

        recordSet = new JdbcRecordSet(jdbcClient, split, ImmutableList.of(
                new JdbcColumnHandle("test", "value", LONG, 1),
                new JdbcColumnHandle("test", "value", LONG, 1),
                new JdbcColumnHandle("test", "text", STRING, 0)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(LONG, LONG, STRING));

        recordSet = new JdbcRecordSet(jdbcClient, split, ImmutableList.<JdbcColumnHandle>of());
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of());
    }

    @Test
    public void testCursorSimple()
            throws Exception
    {
        RecordSet recordSet = new JdbcRecordSet(jdbcClient, split, ImmutableList.of(
                new JdbcColumnHandle("test", "text", STRING, 0),
                new JdbcColumnHandle("test", "value", LONG, 1)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), STRING);
        assertEquals(cursor.getType(1), LONG);

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(new String(cursor.getString(0), UTF_8), cursor.getLong(1));
            assertFalse(cursor.isNull(0));
            assertFalse(cursor.isNull(1));
        }
        assertEquals(data, ImmutableMap.<String, Long>builder()
                .put("one", 1L)
                .put("two", 2L)
                .put("three", 3L)
                .put("ten", 10L)
                .put("eleven", 11L)
                .put("twelve", 12L)
                .build());
    }

    @Test
    public void testCursorMixedOrder()
            throws Exception
    {
        RecordSet recordSet = new JdbcRecordSet(jdbcClient, split, ImmutableList.of(
                new JdbcColumnHandle("test", "value", LONG, 1),
                new JdbcColumnHandle("test", "value", LONG, 1),
                new JdbcColumnHandle("test", "text", STRING, 0)));
        RecordCursor cursor = recordSet.cursor();

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            assertEquals(cursor.getLong(0), cursor.getLong(1));
            data.put(new String(cursor.getString(2), UTF_8), cursor.getLong(0));
        }
        assertEquals(data, ImmutableMap.<String, Long>builder()
                .put("one", 1L)
                .put("two", 2L)
                .put("three", 3L)
                .put("ten", 10L)
                .put("eleven", 11L)
                .put("twelve", 12L)
                .build());
    }

    //
    // Your code should also have test for all types and the state machine of your cursor
    //
}
