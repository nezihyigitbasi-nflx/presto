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
package com.facebook.presto.execution;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.sql.tree.RollbackToSavepoint;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;

import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class RollbackToSavepointTask
        implements DataDefinitionTask<RollbackToSavepoint>
{
    @Override
    public String getName()
    {
        return "ROLLBACK TO SAVEPOINT";
    }

    @Override
    public String explain(RollbackToSavepoint statement)
    {
        return "ROLLBACK TO SAVEPOINT " + statement.getName();
    }

    @Override
    public CompletableFuture<?> execute(RollbackToSavepoint statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine)
    {
        TransactionId transactionId = stateMachine.getRequiredTransactionId();
        transactionManager.rollbackToSavepoint(transactionId, statement.getName());
        return completedFuture(null);
    }

    @Override
    public boolean isTransactionControl()
    {
        return true;
    }
}
