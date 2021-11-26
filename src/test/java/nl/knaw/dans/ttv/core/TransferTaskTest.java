/*
 * Copyright (C) 2021 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.ttv.core;

import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
class TransferTaskTest {

    private final DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
            .addEntityClass(TransferItem.class)
            .build();

    private TransferItemDAO transferItemDAO;
    private ExecutorService executorService;

    private List<Task> tasks;

    @BeforeEach
    void setUp() {
        executorService =
                new ThreadPoolExecutor(1, 5, 60000L, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(5));
        transferItemDAO = new TransferItemDAO(daoTestRule.getSessionFactory());
        Inbox inbox = new UnitOfWorkAwareProxyFactory("UnitOfWorkProxy", daoTestRule.getSessionFactory())
                .create(Inbox.class, new Class[]{String.class, Path.class}, new Object[]{"DvInstance1", Paths.get("src/test/resources/data/DvInstance1")});
        inbox.setSessionFactory(daoTestRule.getSessionFactory());
        inbox.setTransferItemDAO(transferItemDAO);
        tasks = inbox.createTransferItemTasks();
        tasks.sort(Inbox.TASK_QUEUE_DATE_COMPARATOR);
    }

    @Test
    void testMetadataExtractionSuccess() {
        List<Future<String>> futures = new ArrayList<>();
        tasks.forEach(task -> futures.add(executorService.submit(task, "Complete")));
        Objects.requireNonNull(futures).forEach(future -> assertThat(future.isDone()).isTrue());
        final List<TransferItem> transferItems = transferItemDAO.findAllStatusMove();
        assertThat(transferItems).extracting("transferStatus").containsOnly(TransferItem.TransferStatus.MOVE);
    }
}