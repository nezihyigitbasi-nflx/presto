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
package com.facebook.presto.raptor.metadata;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BucketNodeSize
{
    private final int bucketNumber;
    private final String nodeIdentifier;
    private final long size;

    public BucketNodeSize(int bucketNumber, String nodeIdentifier, long size)
    {
        checkArgument(bucketNumber >= 0, "bucket number must be positive");
        checkArgument(size >= 0, "bytes must be positive");
        this.bucketNumber = bucketNumber;
        this.nodeIdentifier = requireNonNull(nodeIdentifier, "nodeIdentifier is null");
        this.size = size;
    }

    public int getBucketNumber()
    {
        return bucketNumber;
    }

    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    public long getSize()
    {
        return size;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucketNumber", bucketNumber)
                .add("nodeIdentifier", nodeIdentifier)
                .add("size", size)
                .toString();
    }

    public static class Mapper
            implements ResultSetMapper<BucketNodeSize>
    {
        @Override
        public BucketNodeSize map(int index, ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new BucketNodeSize(
                    rs.getInt("bucket_number"),
                    rs.getString("node_identifier"),
                    rs.getLong("size"));
        }
    }
}
