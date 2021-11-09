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
package nl.knaw.dans.ttv.tasks;

import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import nl.knaw.dans.ttv.core.Inbox;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
class TransferTaskTest {

    private static final Logger log = LoggerFactory.getLogger(TransferTaskTest.class);

    private final List<Inbox> inboxes = List.of(new Inbox("DvInstance1", Paths.get("src/test/resources/data/DvInstance1")));
    private TransferItemDAO transferItemDAO;
    private TransferTask transferTask;

    public DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
            .addEntityClass(TransferItem.class)
            .build();

    @BeforeEach
    void setUp() throws Exception {
        transferItemDAO = new TransferItemDAO(daoTestRule.getSessionFactory());
        transferTask = new TransferTask(inboxes, transferItemDAO);
    }

    @Test
    void buildTransferQueue() throws Exception {
        transferTask.buildTransferQueue();
        List<TransferItem> itemsOnDisk = transferTask.walkInboxPathsAndFilterDve(inboxes).stream().map(path -> transferTask.dissectDve(path)).collect(Collectors.toList());
        List<TransferItem> transferQueue = transferItemDAO.findAllStatusExtract();
        assertThat(transferQueue).isEqualTo(itemsOnDisk);
    }
}