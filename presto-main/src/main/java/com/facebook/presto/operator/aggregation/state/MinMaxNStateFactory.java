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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.operator.aggregation.TypedHeap;
import com.facebook.presto.spi.function.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.util.array.ObjectBigArray;

public class MinMaxNStateFactory
        implements AccumulatorStateFactory<MinMaxNState>
{
    @Override
    public MinMaxNState createSingleState()
    {
        return new SingleMinMaxNState();
    }

    @Override
    public Class<? extends MinMaxNState> getSingleStateClass()
    {
        return SingleMinMaxNState.class;
    }

    @Override
    public MinMaxNState createGroupedState()
    {
        return new GroupedMinMaxNState();
    }

    @Override
    public Class<? extends MinMaxNState> getGroupedStateClass()
    {
        return GroupedMinMaxNState.class;
    }

    public static class GroupedMinMaxNState
            extends AbstractGroupedAccumulatorState
            implements MinMaxNState
    {
        private final ObjectBigArray<TypedHeap> heaps = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            heaps.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return heaps.sizeOf() + size;
        }

        @Override
        public TypedHeap getTypedHeap()
        {
            return heaps.get(getGroupId());
        }

        @Override
        public void setTypedHeap(TypedHeap value)
        {
            TypedHeap previous = getTypedHeap();
            if (previous != null) {
                size -= previous.getEstimatedSize();
            }
            heaps.set(getGroupId(), value);
            size += value.getEstimatedSize();
        }

        @Override
        public void addMemoryUsage(long memory)
        {
            size += memory;
        }
    }

    public static class SingleMinMaxNState
            implements MinMaxNState
    {
        private TypedHeap typedHeap;

        @Override
        public long getEstimatedSize()
        {
            if (typedHeap == null) {
                return 0;
            }
            return typedHeap.getEstimatedSize();
        }

        @Override
        public TypedHeap getTypedHeap()
        {
            return typedHeap;
        }

        @Override
        public void setTypedHeap(TypedHeap typedHeap)
        {
            this.typedHeap = typedHeap;
        }

        @Override
        public void addMemoryUsage(long memory)
        {
        }
    }
}
