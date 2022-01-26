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
package nl.knaw.dans.ttv.db;

import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@ExtendWith(DropwizardExtensionsSupport.class)
class TransferItemDAOTest {
    private static final Logger log = LoggerFactory.getLogger(TransferItemDAOTest.class);

    private final DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
            .addEntityClass(TransferItem.class)
            .build();

    private TransferItemDAO transferItemDAO;

    @BeforeEach
    void setUp() {
        transferItemDAO = new TransferItemDAO(daoTestRule.getSessionFactory());
    }

    @Test
    void createTransferItem() {
        final TransferItem dataset = daoTestRule.inTransaction(() -> transferItemDAO.save(new TransferItem("doi:10.5072/FK2/P4PHV7", 1, 0, "src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld", LocalDateTime.parse("2007-12-03T10:15:30"), TransferItem.TransferStatus.EXTRACT)));
        assertThat(dataset.getId()).isPositive();
        assertThat(dataset.getDatasetPid()).isEqualTo("doi:10.5072/FK2/P4PHV7");
        assertThat(dataset.getVersionMajor()).isEqualTo(1);
        assertThat(dataset.getVersionMinor()).isEqualTo(0);
        assertThat(dataset.getDveFilePath()).isEqualTo("src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld");
        assertThat(dataset.getCreationTime()).isEqualTo(LocalDateTime.parse("2007-12-03T10:15:30"));
        assertThat(dataset.getTransferStatus()).isEqualTo(TransferItem.TransferStatus.EXTRACT);
        assertThat(transferItemDAO.findById(dataset.getId())).isEqualTo(Optional.of(dataset));
    }

    @Test
    void findAll() {
        daoTestRule.inTransaction(() -> {
            transferItemDAO.save(new TransferItem("doi:10.5072/FK2/P4PHV7", 1, 0, "src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld", LocalDateTime.parse("2007-12-03T10:15:30"), TransferItem.TransferStatus.EXTRACT));
            transferItemDAO.save(new TransferItem("doi:10.5072/FK2/JOY8UU", 2, 0, "src/test/resources/doi-10-5072-fk2-joy8uuv-2-0/metadata/oai-ore.jsonld", LocalDateTime.parse("2008-12-03T11:30:00"), TransferItem.TransferStatus.COLLECTED));
            transferItemDAO.save(new TransferItem("doi:10.5072/FK2/QZ0LJQ", 1, 2, "src/test/resources/doi-10-5072-fk2-qz0ljqv-1-2/metadata/oai-ore.jsonld", LocalDateTime.parse("2020-08-03T00:15:22"), TransferItem.TransferStatus.TARRING));
        });

        final List<TransferItem> transferItems = transferItemDAO.findAll();
        assertThat(transferItems).extracting("datasetPid").containsOnly("doi:10.5072/FK2/P4PHV7", "doi:10.5072/FK2/JOY8UU", "doi:10.5072/FK2/QZ0LJQ");
        assertThat(transferItems).extracting("versionMajor").containsOnly(1, 2);
        assertThat(transferItems).extracting("versionMinor").containsOnly(0, 2);
        assertThat(transferItems).extracting("dveFilePath").containsOnly("src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld", "src/test/resources/doi-10-5072-fk2-joy8uuv-2-0/metadata/oai-ore.jsonld", "src/test/resources/doi-10-5072-fk2-qz0ljqv-1-2/metadata/oai-ore.jsonld");
        assertThat(transferItems).extracting("creationTime").containsOnly(LocalDateTime.parse("2007-12-03T10:15:30"), LocalDateTime.parse("2008-12-03T11:30:00"), LocalDateTime.parse("2020-08-03T00:15:22"));
        assertThat(transferItems).extracting("transferStatus").containsOnly(TransferItem.TransferStatus.EXTRACT, TransferItem.TransferStatus.COLLECTED, TransferItem.TransferStatus.TARRING);
    }

    @Test
    void handlesNullDatasetPid() {
        assertThatExceptionOfType(ConstraintViolationException.class).isThrownBy(() ->
                daoTestRule.inTransaction(() -> transferItemDAO.save(new TransferItem(null, 1, 0, "src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld", LocalDateTime.parse("2020-08-03T00:15:22"), TransferItem.TransferStatus.EXTRACT))));
    }

    @Test
    void findById() {
    }

    @Test
    void save() {
    }

    @Test
    void findAllStatusExtract() {
    }

    @Test
    void findAllStatusMove() {
    }

    @Test
    void findAllStatusTar() {
    }

    @Test
    void findAllStatusTarring() {
    }

    @Test
    void merge() {
    }

    @Test
    void flush() {
    }

    @Test
    void findByDvePath() {
    }

    @Test
    void findByStatus() {
    }

    @Test
    void findAllByTarId() {
    }

    @Test
    void updateStatusByTar() {
    }

    @Test
    void findByDatasetPidAndVersion() {
    }

    @Test
    void findAllTarsToBeConfirmed() {
        daoTestRule.inTransaction(() -> {
            var item1 = new TransferItem("doi:10.5072/FK2/P4PHV7", 1, 0, "src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld", LocalDateTime.parse("2007-12-03T10:15:30"), TransferItem.TransferStatus.OCFLTARCREATED);
            var item2 = new TransferItem("doi:10.5073/FK2/P4PHV7", 1, 0, "src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld", LocalDateTime.parse("2007-12-03T10:15:30"), TransferItem.TransferStatus.OCFLTARCREATED);
            item2.setConfirmCheckInProgress(true);
            var item3 = new TransferItem("doi:10.5074/FK2/P4PHV7", 1, 0, "src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld", LocalDateTime.parse("2007-12-03T10:15:30"), TransferItem.TransferStatus.COLLECTED);
            transferItemDAO.save(item1);
            transferItemDAO.save(item2);
            transferItemDAO.save(item3);
        });

        final List<TransferItem> transferItems = transferItemDAO.findAllTarsToBeConfirmed();
        assertThat(transferItems).extracting("datasetPid")
            .containsOnly("doi:10.5072/FK2/P4PHV7");
        assertThat(transferItems).extracting("confirmCheckInProgress")
            .containsOnly(false);

    }

    @Test
    void updateCheckingProgressResults() {

    }
}
