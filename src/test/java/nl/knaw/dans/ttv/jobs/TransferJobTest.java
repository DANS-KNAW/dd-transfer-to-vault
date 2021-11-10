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
package nl.knaw.dans.ttv.jobs;

import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import nl.knaw.dans.ttv.core.Inbox;
import nl.knaw.dans.ttv.core.InvalidTransferItemException;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@ExtendWith(DropwizardExtensionsSupport.class)
class TransferJobTest {

    private static final Logger log = LoggerFactory.getLogger(TransferJobTest.class);

    private final List<Inbox> inboxes = Collections.singletonList(new Inbox("DvInstance1", Paths.get("src/test/resources/data/DvInstance1")));
    private TransferItemDAO transferItemDAO;
    private TransferJob transferJob;

    public DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
            .addEntityClass(TransferItem.class)
            .build();

    @BeforeEach
    void setUp() throws Exception {
        transferItemDAO = new TransferItemDAO(daoTestRule.getSessionFactory());
        transferJob = new TransferJob(inboxes, transferItemDAO);
    }

    @Test
    void buildTransferQueue() throws Exception {
        transferJob.buildTransferQueue();
        List<TransferItem> itemsOnDisk = transferJob.walkInboxPathsAndFilterDve(inboxes).stream().map(path -> transferJob.dissectDve(path)).collect(Collectors.toList());
        List<TransferItem> transferQueue = transferItemDAO.findAllStatusExtract();
        assertThat(transferQueue).isEqualTo(itemsOnDisk);
    }

    @Test
    void failedConsistencyCheck() throws Exception{
        daoTestRule.inTransaction(() -> {
            transferItemDAO.create(new TransferItem("doi:10.5072/FK2/P4PHV7", 1, 0, "src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld", LocalDateTime.parse("2007-12-03T10:15:30"), TransferItem.TransferStatus.EXTRACT));
            transferItemDAO.create(new TransferItem("doi:10.5072/FK2/JOY8UU", 2, 0, "src/test/resources/doi-10-5072-fk2-joy8uuv-2-0/metadata/oai-ore.jsonld", LocalDateTime.parse("2008-12-03T11:30:00"), TransferItem.TransferStatus.EXTRACT));
            transferItemDAO.create(new TransferItem("doi:10.5072/FK2/QZ0LJQ", 1, 2, "src/test/resources/doi-10-5072-fk2-qz0ljqv-1-2/metadata/oai-ore.jsonld", LocalDateTime.parse("2020-08-03T00:15:22"), TransferItem.TransferStatus.EXTRACT));
        });
        assertThatExceptionOfType(InvalidTransferItemException.class).isThrownBy(() -> transferJob.buildTransferQueue());
    }
}