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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.hive.$internal.jodd.datetime.JDateTime;
import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.Test;
import parquet.io.api.Binary;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.parquet.ParquetTimestampUtils.getTimestampMillis;
import static com.facebook.presto.hive.parquet.ParquetTimestampUtils.toBinary;
import static org.testng.Assert.assertEquals;

public class TestParquetTimestampUtils
{
    private static final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
    private static final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
    private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

    @Test
    public void testGetTimestampMillis()
    {
        assertTimestampCorrect("2011-01-01 00:00:00.000000000");
        assertTimestampCorrect("2001-01-01 01:01:01.000000001");
        assertTimestampCorrect("2015-12-31 23:59:59.999999999");
    }

    @Test
    public void testInvalidBinaryLength()
    {
        try {
            byte[] invalidLengthBinaryTimestamp = new byte[8];
            getTimestampMillis(Binary.fromByteArray(invalidLengthBinaryTimestamp));
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), HIVE_BAD_DATA.toErrorCode());
            assertEquals(e.getMessage(), "Parquet timestamp must be 12 bytes, actual 8");
        }
    }

    private static void assertTimestampCorrect(String timestampString)
    {
        Timestamp timestamp = Timestamp.valueOf(timestampString);
        Binary timestampBytes = toParquetBinary(timestamp);
        long decodedTimestampMillis = getTimestampMillis(timestampBytes);
        assertEquals(decodedTimestampMillis, timestamp.getTime());
    }

    // copied from Hive NanoTimeUtils
    private static Binary toParquetBinary(Timestamp timestamp)
    {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        calendar.setTime(timestamp);
        JDateTime jDateTime = new JDateTime(calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH) + 1,  //java calendar index starting at 1.
                calendar.get(Calendar.DAY_OF_MONTH));
        int days = jDateTime.getJulianDayNumber();

        long hour = calendar.get(Calendar.HOUR_OF_DAY);
        long minute = calendar.get(Calendar.MINUTE);
        long second = calendar.get(Calendar.SECOND);
        long nanos = timestamp.getNanos();
        long nanosOfDay = nanos + NANOS_PER_SECOND * second + NANOS_PER_MINUTE * minute +
                NANOS_PER_HOUR * hour;
        return toBinary(days, nanosOfDay);
    }
}
