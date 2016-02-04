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
package com.facebook.presto.transaction;

import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.StandardErrorCode.SAVEPOINT_NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_ALREADY_ABORTED;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_ROLLBACK;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTransactionManager
{
    private final ExecutorService finishingExecutor = newCachedThreadPool(daemonThreadsNamed("transaction-%s"));

    @AfterClass
    public void tearDown()
            throws Exception
    {
        finishingExecutor.shutdownNow();
    }

    @Test
    public void testTransactionWorkflow()
            throws Exception
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            TransactionManager transactionManager = TransactionManager.create(new TransactionManagerConfig(), executor.getExecutor(), finishingExecutor);

            Connector c1 = new TpchConnectorFactory(new InMemoryNodeManager()).create("c1", ImmutableMap.of());
            transactionManager.addConnector("c1", c1);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertFalse(transactionInfo.isAutoCommitContext());
            assertTrue(transactionInfo.getConnectorIds().isEmpty());
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            ConnectorMetadata metadata = transactionManager.getMetadata(transactionId, "c1");
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession("c1"));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertEquals(transactionInfo.getConnectorIds(), ImmutableList.of("c1"));
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            transactionManager.asyncCommit(transactionId).join();

            assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    @Test
    public void testAbortedTransactionWorkflow()
            throws Exception
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            TransactionManager transactionManager = TransactionManager.create(new TransactionManagerConfig(), executor.getExecutor(), finishingExecutor);

            Connector c1 = new TpchConnectorFactory(new InMemoryNodeManager()).create("c1", ImmutableMap.of());
            transactionManager.addConnector("c1", c1);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertFalse(transactionInfo.isAutoCommitContext());
            assertTrue(transactionInfo.getConnectorIds().isEmpty());
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            ConnectorMetadata metadata = transactionManager.getMetadata(transactionId, "c1");
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession("c1"));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertEquals(transactionInfo.getConnectorIds(), ImmutableList.of("c1"));
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            transactionManager.asyncAbort(transactionId).join();

            assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    @Test
    public void testFailedTransactionWorkflow()
            throws Exception
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            TransactionManager transactionManager = TransactionManager.create(new TransactionManagerConfig(), executor.getExecutor(), finishingExecutor);

            Connector c1 = new TpchConnectorFactory(new InMemoryNodeManager()).create("c1", ImmutableMap.of());
            transactionManager.addConnector("c1", c1);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertFalse(transactionInfo.isAutoCommitContext());
            assertTrue(transactionInfo.getConnectorIds().isEmpty());
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            ConnectorMetadata metadata = transactionManager.getMetadata(transactionId, "c1");
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession("c1"));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertEquals(transactionInfo.getConnectorIds(), ImmutableList.of("c1"));
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            transactionManager.fail(transactionId);
            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

            try {
                transactionManager.getMetadata(transactionId, "c1");
                fail();
            }
            catch (PrestoException e) {
                assertEquals(e.getErrorCode(), TRANSACTION_ALREADY_ABORTED.toErrorCode());
            }
            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

            transactionManager.asyncAbort(transactionId).join();

            assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    @Test
    public void testExpiration()
            throws Exception
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            TransactionManager transactionManager = TransactionManager.create(
                    new TransactionManagerConfig()
                            .setIdleTimeout(new Duration(1, TimeUnit.MILLISECONDS))
                            .setIdleCheckInterval(new Duration(5, TimeUnit.MILLISECONDS)),
                    executor.getExecutor(),
                    finishingExecutor);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertFalse(transactionInfo.isAutoCommitContext());
            assertTrue(transactionInfo.getConnectorIds().isEmpty());
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            transactionManager.trySetInactive(transactionId);
            TimeUnit.MILLISECONDS.sleep(100);

            assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    @Test
    public void testForcedRollback()
            throws Exception
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            TransactionManager transactionManager = TransactionManager.create(new TransactionManagerConfig(), executor.getExecutor(), finishingExecutor);

            Connector test = new TpchConnectorFactory(new InMemoryNodeManager()).create("test", ImmutableMap.of());
            transactionManager.addConnector("test", test);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            transactionManager.fail(transactionId);

            try {
                transactionManager.asyncCommit(transactionId).join();
                fail("expected exception");
            }
            catch (CompletionException e) {
                assertInstanceOf(e.getCause(), PrestoException.class);
                assertEquals(((PrestoException) e.getCause()).getErrorCode(), TRANSACTION_ROLLBACK.toErrorCode());
            }
        }
    }

    @Test
    public void testSavepoints()
            throws Exception
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            TransactionManager transactionManager = TransactionManager.create(new TransactionManagerConfig(), executor.getExecutor(), finishingExecutor);

            Connector test = new TpchConnectorFactory(new InMemoryNodeManager()).create("test", ImmutableMap.of());
            transactionManager.addConnector("test", test);

            // begin transaction
            TransactionId transactionId = transactionManager.beginTransaction(false);

            // create savepoint before selecting the writable connector
            transactionManager.savepoint(transactionId, "apple");

            // select the writable connector
            transactionManager.getMetadata(transactionId, "test");
            transactionManager.checkConnectorWrite(transactionId, "test");

            // create more savepoints
            transactionManager.savepoint(transactionId, "orange");
            transactionManager.savepoint(transactionId, "grape");

            // fail the transaction
            transactionManager.fail(transactionId);

            // verify we are failed
            assertTransactionAborted(transactionManager, transactionId);

            // rollback to the middle savepoint
            transactionManager.rollbackToSavepoint(transactionId, "orange");

            // verify we are not failed
            transactionManager.checkAndSetActive(transactionId);

            // verify previous top savepoint is not available
            assertSavepointNotFound(() -> transactionManager.rollbackToSavepoint(transactionId, "grape"));
            assertSavepointNotFound(() -> transactionManager.releaseSavepoint(transactionId, "grape"));

            // release most recent savepoint
            transactionManager.releaseSavepoint(transactionId, "orange");

            // verify savepoint is not available
            assertSavepointNotFound(() -> transactionManager.rollbackToSavepoint(transactionId, "orange"));
            assertSavepointNotFound(() -> transactionManager.releaseSavepoint(transactionId, "orange"));

            // fail the transaction
            transactionManager.fail(transactionId);

            // verify we are failed
            assertTransactionAborted(transactionManager, transactionId);

            // rollback to first savepoint
            transactionManager.rollbackToSavepoint(transactionId, "apple");

            // verify we are not failed
            transactionManager.checkAndSetActive(transactionId);

            // verify savepoint is not available
            assertSavepointNotFound(() -> transactionManager.rollbackToSavepoint(transactionId, "orange"));
            assertSavepointNotFound(() -> transactionManager.releaseSavepoint(transactionId, "orange"));

            // commit transaction
            transactionManager.asyncCommit(transactionId).join();
        }
    }

    private static void assertTransactionAborted(TransactionManager transactionManager, TransactionId transactionId)
    {
        try {
            transactionManager.checkAndSetActive(transactionId);
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), TRANSACTION_ALREADY_ABORTED.toErrorCode());
        }
    }

    private static void assertSavepointNotFound(Runnable runnable)
    {
        try {
            runnable.run();
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), SAVEPOINT_NOT_FOUND.toErrorCode());
        }
    }

    private static class IdleCheckExecutor
            implements Closeable
    {
        private final ScheduledExecutorService executorService = newSingleThreadScheduledExecutor(daemonThreadsNamed("idle-check"));

        public ScheduledExecutorService getExecutor()
        {
            return executorService;
        }

        @Override
        public void close()
                throws IOException
        {
            executorService.shutdownNow();
        }
    }
}
