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

public final class Merge
        extends Statement
{
    private final QualifiedName target;
    private final Optional<String> targetAlias;
    private final Relation relation;
    private final Expression expression;
    private final List<MergeOperation> operations;

    public Merge(
            QualifiedName target,
            Optional<String> targetAlias,
            Relation relation,
            Expression expression,
            List<MergeOperation> operations)
    {
        this.target = requireNonNull(target, "target is null");
        this.targetAlias = requireNonNull(targetAlias, "targetAlias is null");
        this.relation = requireNonNull(relation, "relation is null");
        this.expression = requireNonNull(expression, "expression is null");
        this.operations = ImmutableList.copyOf(requireNonNull(operations, "operations is null"));
    }

    public QualifiedName getTarget()
    {
        return target;
    }

    public Optional<String> getTargetAlias()
    {
        return targetAlias;
    }

    public Relation getRelation()
    {
        return relation;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public List<MergeOperation> getOperations()
    {
        return operations;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMerge(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(target, relation, operations);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Merge o = (Merge) obj;
        return Objects.equals(target, o.target) &&
                Objects.equals(targetAlias, o.targetAlias) &&
                Objects.equals(relation, o.relation) &&
                Objects.equals(expression, o.expression) &&
                Objects.equals(operations, o.operations);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("target", target)
                .add("targetAlias", targetAlias)
                .add("relation", relation)
                .add("expression", expression)
                .add("operations", operations)
                .toString();
    }
}
