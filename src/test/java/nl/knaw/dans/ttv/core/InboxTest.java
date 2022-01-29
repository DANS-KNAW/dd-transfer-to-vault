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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import nl.knaw.dans.ttv.db.TransferItem;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(DropwizardExtensionsSupport.class)
class InboxTest {

    private static final Logger log = LoggerFactory.getLogger(InboxTest.class);

    private final DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
        .addEntityClass(TransferItem.class)
        .build();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private TransferItemDAO transferItemDAO;

    @BeforeEach
    void setUp() {
        transferItemDAO = new TransferItemDAO(daoTestRule.getSessionFactory());
        //        inbox = new Inbox("inbox", Paths.get("src/test/resources/data/inbox"), transferItemDAO, ocflRepositoryManager, daoTestRule.getSessionFactory(), objectMapper);
    }
/*
    @Test
    void createTransferItemTasks() {
        List<Task> transferItemTasks = inbox.createTransferItemTasks();
        assertThat(transferItemTasks).extracting("transferItem").extracting("datasetPid").containsOnly("10.5072/DAR/MIHEZ7", "10.5072/DAR/KXTEQT", "10.5072/DAR/VFSPUQ", "10.5072/DAR/XZNG4N");
        assertThat(transferItemTasks).extracting("transferItem").extracting("versionMajor").containsOnly(1);
        assertThat(transferItemTasks).extracting("transferItem").extracting("versionMinor").containsOnly(0);
        assertThat(transferItemTasks).extracting("transferItem").extracting("dveFilePath").containsOnly("src/test/resources/data/inbox/doi-10-5072-dar-mihez7v1.0.zip", "src/test/resources/data/inbox/doi-10-5072-dar-kxteqtv1.0.zip", "src/test/resources/data/inbox/doi-10-5072-dar-vfspuqv1.0.zip", "src/test/resources/data/inbox/doi-10-5072-dar-xzng4nv1.0.zip");
        assertThat(transferItemTasks).extracting("transferItem").extracting("transferStatus").containsOnly(TransferItem.TransferStatus.EXTRACT);
    }

    @Test
    void taskQueueDateComparatorTest() {
        daoTestRule.inTransaction(() -> {
            transferItemDAO.save(new TransferItem("doi:10.5072/FK2/P4PHV7", 1, 0, "src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld", LocalDateTime.parse("2007-12-03T10:15:30"), TransferItem.TransferStatus.EXTRACT));
            transferItemDAO.save(new TransferItem("doi:10.5072/FK2/JOY8UU", 2, 0, "src/test/resources/doi-10-5072-fk2-joy8uuv-2-0/metadata/oai-ore.jsonld", LocalDateTime.parse("2008-12-03T11:30:00"), TransferItem.TransferStatus.EXTRACT));
            transferItemDAO.save(new TransferItem("doi:10.5072/FK2/QZ0LJQ", 1, 2, "src/test/resources/doi-10-5072-fk2-qz0ljqv-1-2/metadata/oai-ore.jsonld", LocalDateTime.parse("2020-08-03T00:15:22"), TransferItem.TransferStatus.EXTRACT));
        });
        List<Task> sortedTransferItemTasks = transferItemDAO.findAllStatusExtract().stream()
                .map(transferItem -> new TransferTask(transferItem, transferItemDAO, ocflRepositoryManager, objectMapper))
                .sorted(Inbox.TASK_QUEUE_DATE_COMPARATOR)
                .collect(Collectors.toList());
        assertThat(sortedTransferItemTasks.get(0).getTransferItem().getCreationTime()).isEqualTo(LocalDateTime.parse("2007-12-03T10:15:30"));
        assertThat(sortedTransferItemTasks.get(1).getTransferItem().getCreationTime()).isEqualTo(LocalDateTime.parse("2008-12-03T11:30:00"));
        assertThat(sortedTransferItemTasks.get(2).getTransferItem().getCreationTime()).isEqualTo(LocalDateTime.parse("2020-08-03T00:15:22"));
    }

    @Test
    void failedConsistencyCheck() {
        daoTestRule.inTransaction(() -> {
            transferItemDAO.save(new TransferItem("doi:10.5072/FK2/P4PHV7", 1, 0, "src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld", LocalDateTime.parse("2007-12-03T10:15:30"), TransferItem.TransferStatus.EXTRACT));
            transferItemDAO.save(new TransferItem("doi:10.5072/FK2/JOY8UU", 2, 0, "src/test/resources/doi-10-5072-fk2-joy8uuv-2-0/metadata/oai-ore.jsonld", LocalDateTime.parse("2008-12-03T11:30:00"), TransferItem.TransferStatus.EXTRACT));
            transferItemDAO.save(new TransferItem("doi:10.5072/FK2/QZ0LJQ", 1, 2, "src/test/resources/doi-10-5072-fk2-qz0ljqv-1-2/metadata/oai-ore.jsonld", LocalDateTime.parse("2020-08-03T00:15:22"), TransferItem.TransferStatus.EXTRACT));
        });
        assertThatExceptionOfType(InvalidTransferItemException.class).isThrownBy(inbox::createTransferItemTasks);
    }

 */
}
