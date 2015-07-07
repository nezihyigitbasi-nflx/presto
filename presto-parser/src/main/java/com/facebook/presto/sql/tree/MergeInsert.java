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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MergeInsert
        extends MergeOperation
{
    private final List<String> columns;
    private final List<Expression> values;

    public MergeInsert(Optional<Expression> expression, List<String> columns, List<Expression> values)
    {
        super(expression);
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.values = ImmutableList.copyOf(requireNonNull(values, "values is null"));
    }

    public List<String> getColumns()
    {
        return columns;
    }

    public List<Expression> getValues()
    {
        return values;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMergeInsert(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, columns, values);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MergeInsert o = (MergeInsert) obj;
        return Objects.equals(expression, o.expression) &&
                Objects.equals(columns, o.columns) &&
                Objects.equals(values, o.values);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression.orElse(null))
                .add("columns", columns)
                .add("values", values)
                .omitNullValues()
                .toString();
    }
}
