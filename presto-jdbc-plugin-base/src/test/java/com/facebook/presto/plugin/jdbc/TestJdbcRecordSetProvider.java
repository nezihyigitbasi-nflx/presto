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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.SplitSource;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.plugin.jdbc.TestingJdbcClient.createTestingJdbcClient;
import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestJdbcRecordSetProvider
{
    private JdbcClient jdbcClient;
    private JdbcSplit split;

    private JdbcTableHandle table;
    private JdbcColumnHandle textColumn = new JdbcColumnHandle("test", "text", ColumnType.STRING, 0);
    private JdbcColumnHandle valueColumn = new JdbcColumnHandle("test", "value", ColumnType.LONG, 1);

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        jdbcClient = createTestingJdbcClient("test" + System.nanoTime());
        split = TestingJdbcClient.getSplit(jdbcClient, "example", "numbers");

        table = jdbcClient.getTableHandle(new SchemaTableName("example", "numbers"));
    }

    @Test
    public void testCanHandle()
    {
        JdbcRecordSetProvider recordSetProvider = new JdbcRecordSetProvider(new JdbcConnectorId("test"), jdbcClient);
        assertTrue(recordSetProvider.canHandle(split));
        assertFalse(recordSetProvider.canHandle(new JdbcSplit("unknown", "catalog", "schema", "table", "connectionUrl", ImmutableMap.<String, String>of(), TupleDomain.all())));
    }

    @Test
    public void testGetRecordSet()
            throws Exception
    {
        JdbcRecordSetProvider recordSetProvider = new JdbcRecordSetProvider(new JdbcConnectorId("test"), jdbcClient);
        RecordSet recordSet = recordSetProvider.getRecordSet(split, ImmutableList.of(
                new JdbcColumnHandle("test", "text", ColumnType.STRING, 0),
                new JdbcColumnHandle("test", "value", ColumnType.LONG, 1)));
        Assert.assertNotNull(recordSet, "recordSet is null");

        RecordCursor cursor = recordSet.cursor();
        Assert.assertNotNull(cursor, "cursor is null");

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(new String(cursor.getString(0), UTF_8), cursor.getLong(1));
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
    public void testTupleDomain()
            throws Exception

    {
        // single value
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.singleValue("foo"))
        ));

        // multiple values (string)
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.union(ImmutableList.of(Domain.singleValue("foo"), Domain.singleValue("bar"))))
        ));

        // inequality (string)
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(SortedRangeSet.of(Range.greaterThan("foo")), false))
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(SortedRangeSet.of(Range.greaterThan("foo")), false))
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(SortedRangeSet.of(Range.lessThanOrEqual("foo")), false))
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(SortedRangeSet.of(Range.lessThan("foo")), false))
        ));

        // is null
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.onlyNull(String.class))
        ));

        // not null
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.notNull(String.class))
        ));

        // specific value or null
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.union(ImmutableList.of(Domain.singleValue("foo"), Domain.onlyNull(String.class))))
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(SortedRangeSet.of(Range.range("bar", true, "foo", true)), false))
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(SortedRangeSet.of(
                                Range.range("bar", true, "foo", true),
                                Range.range("hello", false, "world", false)),
                        false
                ))
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(
                        textColumn,
                        Domain.create(SortedRangeSet.of(
                                        Range.range("bar", true, "foo", true),
                                        Range.range("hello", false, "world", false),
                                        Range.equal("apple"),
                                        Range.equal("banana"),
                                        Range.equal("zoo")),
                                false
                        ),

                        valueColumn,
                        Domain.create(SortedRangeSet.of(
                                        Range.range(1, true, 5, true),
                                        Range.range(10, false, 20, false)),
                                true
                        )
                )
        ));
    }

    private RecordCursor getCursor(JdbcTableHandle jdbcTableHandle, List<JdbcColumnHandle> columns, TupleDomain domain)
            throws InterruptedException
    {
        PartitionResult partitions = jdbcClient.getPartitions(jdbcTableHandle, domain);
        SplitSource splits = jdbcClient.getPartitionSplits((JdbcPartition) Iterables.getOnlyElement(partitions.getPartitions()));
        JdbcSplit split = (JdbcSplit) Iterables.getOnlyElement(splits.getNextBatch(1000));

        JdbcRecordSetProvider recordSetProvider = new JdbcRecordSetProvider(new JdbcConnectorId("test"), jdbcClient);
        RecordSet recordSet = recordSetProvider.getRecordSet(split, columns);

        return recordSet.cursor();
    }
}
