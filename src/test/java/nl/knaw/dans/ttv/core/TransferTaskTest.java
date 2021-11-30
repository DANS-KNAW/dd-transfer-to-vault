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

import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
class TransferTaskTest {

    private final DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
            .setUrl("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE")
            .addEntityClass(TransferItem.class)
            .build();

    private final Inbox inbox = new Inbox("DvInstance1", Paths.get("src/test/resources/data/DvInstance1"));
    TransferItemDAO transferItemDAO;
    ExecutorService executorService;

    private List<Task> tasks;

    @BeforeEach
    void setUp() {
        executorService =
                new ThreadPoolExecutor(1, 5, 60000L, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(5));
        transferItemDAO = new TransferItemDAO(daoTestRule.getSessionFactory());
        inbox.setSessionFactory(daoTestRule.getSessionFactory());
        inbox.setTransferItemDAO(transferItemDAO);
        tasks = inbox.createTransferItemTasks();
        tasks.sort(Inbox.TASK_QUEUE_DATE_COMPARATOR);
    }

    @Test
    void run() throws Exception {
        /*List<Future<TransferItem>> futures = executorService.invokeAll(tasks);
        Objects.requireNonNull(futures).forEach(transferItemFuture -> {
            assertThat(transferItemFuture.isDone()).isTrue();
            try {
                assertThat(transferItemFuture.get().getPidMapping()).isNotNull();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });*/
        /*tasks.forEach(task -> executorService.execute(task));
        List<TransferItem> withStatusMove = transferItemDAO.findAllStatusExtract();
        withStatusMove.forEach(System.out::println);*/

    }
}