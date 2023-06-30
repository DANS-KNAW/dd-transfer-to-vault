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
import nl.knaw.dans.ttv.core.domain.Version;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@ExtendWith(DropwizardExtensionsSupport.class)
class TransferItemDAOTest {
    private static final Logger log = LoggerFactory.getLogger(TransferItemDAOTest.class);

    private final DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
        .addEntityClass(TransferItem.class)
        .addEntityClass(Tar.class)
        .addEntityClass(TarPart.class)
        .build();

    private TransferItemDAO transferItemDAO;

    @BeforeEach
    void setUp() {
        transferItemDAO = new TransferItemDAO(daoTestRule.getSessionFactory());
    }

    @Test
    void createTransferItem() {
        final TransferItem dataset = daoTestRule.inTransaction(() -> transferItemDAO.save(
            TransferItem.builder()
                .datasetPid("doi:10.5072/FK2/P4PHV7")
                .datasetVersion("1.0")
                .dveFilePath("src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld")
                .creationTime(OffsetDateTime.parse("2007-12-03T10:15:30Z"))
                .transferStatus(TransferItem.TransferStatus.METADATA_EXTRACTED)
                .build()));

        assertThat(dataset.getId()).isPositive();
        assertThat(dataset.getDatasetPid()).isEqualTo("doi:10.5072/FK2/P4PHV7");
        assertThat(dataset.getDatasetVersion()).isEqualTo("1.0");
        assertThat(dataset.getDveFilePath()).isEqualTo("src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld");
        assertThat(dataset.getCreationTime()).isEqualTo(OffsetDateTime.parse("2007-12-03T10:15:30Z"));
        assertThat(dataset.getTransferStatus()).isEqualTo(TransferItem.TransferStatus.METADATA_EXTRACTED);
        assertThat(transferItemDAO.findById(dataset.getId())).isEqualTo(Optional.of(dataset));
    }

    @Test
    void findAll() {
        daoTestRule.inTransaction(() -> {

            transferItemDAO.save(TransferItem.builder()
                .datasetPid("doi:10.5072/FK2/P4PHV7")
                .datasetVersion("1.0")
                .dveFilePath("src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld")
                .creationTime(OffsetDateTime.parse("2007-12-03T10:15:30Z"))
                .transferStatus(TransferItem.TransferStatus.METADATA_EXTRACTED)
                .build()
            );

            transferItemDAO.save(TransferItem.builder()
                .datasetPid("doi:10.5072/FK2/JOY8UU")
                .datasetVersion("2.0")
                .dveFilePath("src/test/resources/doi-10-5072-fk2-joy8uuv-2-0/metadata/oai-ore.jsonld")
                .creationTime(OffsetDateTime.parse("2008-12-03T11:30:00Z"))
                .transferStatus(TransferItem.TransferStatus.METADATA_EXTRACTED)
                .build()
            );

            transferItemDAO.save(TransferItem.builder()
                .datasetPid("doi:10.5072/FK2/QZ0LJQ")
                .datasetVersion("2.0")
                .dveFilePath("src/test/resources/doi-10-5072-fk2-qz0ljqv-1-2/metadata/oai-ore.jsonld")
                .creationTime(OffsetDateTime.parse("2020-08-03T00:15:22Z"))
                .transferStatus(TransferItem.TransferStatus.TARRING)
                .build()
            );
        });

        final List<TransferItem> transferItems = transferItemDAO.findAll();
        assertThat(transferItems).extracting("datasetPid").containsOnly("doi:10.5072/FK2/P4PHV7", "doi:10.5072/FK2/JOY8UU", "doi:10.5072/FK2/QZ0LJQ");
        assertThat(transferItems).extracting("datasetVersion").containsOnly("1.0", "2.0");
//        assertThat(transferItems).extracting("versionMinor").containsOnly(0, 2);
        assertThat(transferItems).extracting("dveFilePath")
            .containsOnly("src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld", "src/test/resources/doi-10-5072-fk2-joy8uuv-2-0/metadata/oai-ore.jsonld",
                "src/test/resources/doi-10-5072-fk2-qz0ljqv-1-2/metadata/oai-ore.jsonld");
        assertThat(transferItems).extracting("creationTime")
            .containsOnly(OffsetDateTime.parse("2007-12-03T10:15:30Z"), OffsetDateTime.parse("2008-12-03T11:30:00Z"), OffsetDateTime.parse("2020-08-03T00:15:22Z"));
        assertThat(transferItems).extracting("transferStatus").containsOnly(TransferItem.TransferStatus.METADATA_EXTRACTED, TransferItem.TransferStatus.TARRING);
    }

    @Test
    void handlesNullDatasetPid() {
        assertThatExceptionOfType(ConstraintViolationException.class).isThrownBy(() ->
            daoTestRule.inTransaction(() -> {
                transferItemDAO.save(TransferItem.builder()
                    .datasetVersion("1.0")
                    .dveFilePath("src/test/resources/doi-10-5072-fk2-qz0ljqv-1-2/metadata/oai-ore.jsonld")
                    .transferStatus(TransferItem.TransferStatus.METADATA_EXTRACTED)
                    .build()
                );
            }));
    }

    @Test
    void findByDatasetPidAndVersion() {
        daoTestRule.inTransaction(() -> {

            transferItemDAO.save(TransferItem.builder()
                .datasetPid("doi:10.5072/FK2/P4PHV7")
                .versionMajor(1)
                .versionMinor(0)
                .dveFilePath("src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld")
                .creationTime(OffsetDateTime.parse("2007-12-03T10:15:30Z"))
                .transferStatus(TransferItem.TransferStatus.METADATA_EXTRACTED)
                .build()
            );

            transferItemDAO.save(TransferItem.builder()
                .datasetPid("doi:10.5072/FK2/JOY8UU")
                .versionMajor(2)
                .versionMinor(0)
                .dveFilePath("src/test/resources/doi-10-5072-fk2-joy8uuv-2-0/metadata/oai-ore.jsonld")
                .creationTime(OffsetDateTime.parse("2008-12-03T11:30:00Z"))
                .transferStatus(TransferItem.TransferStatus.METADATA_EXTRACTED)
                .build()
            );

            transferItemDAO.save(TransferItem.builder()
                .datasetPid("doi:10.5072/FK2/QZ0LJQ")
                .versionMajor(1)
                .versionMinor(1)
                .dveFilePath("src/test/resources/doi-10-5072-fk2-qz0ljqv-1-2/metadata/oai-ore.jsonld")
                .creationTime(OffsetDateTime.parse("2020-08-03T11:30:00Z"))
                .transferStatus(TransferItem.TransferStatus.TARRING)
                .build()
            );
        });

        var item = transferItemDAO.findByDatasetPidAndVersion("doi:10.5072/FK2/JOY8UU", Version.of(2, 0)).get();
        assertThat(item.getDatasetPid()).isEqualTo("doi:10.5072/FK2/JOY8UU");
        assertThat(item.getVersion()).isEqualTo(Version.of(2, 0));

        var noMatch = transferItemDAO.findByDatasetPidAndVersion("doi:10.5072/FK2/JOY8UU", Version.of(3, 0));
        assertThat(noMatch.isPresent()).isEqualTo(false);
    }

    @Test
    void findByStatus() {
        daoTestRule.inTransaction(() -> {
            transferItemDAO.save(TransferItem.builder()
                .datasetPid("doi:10.5072/FK2/P4PHV7")
                .datasetVersion("1.0")
                .dveFilePath("src/test/resources/doi-10-5072-fk2-p4phv7v-1-0/metadata/oai-ore.jsonld")
                .creationTime(OffsetDateTime.parse("2007-12-03T10:15:30Z"))
                .transferStatus(TransferItem.TransferStatus.METADATA_EXTRACTED)
                .build()
            );

            transferItemDAO.save(TransferItem.builder()
                .datasetPid("doi:10.5072/FK2/JOY8UU")
                .datasetVersion("2.0")
                .dveFilePath("src/test/resources/doi-10-5072-fk2-joy8uuv-2-0/metadata/oai-ore.jsonld")
                .creationTime(OffsetDateTime.parse("2008-12-03T11:30:00Z"))
                .transferStatus(TransferItem.TransferStatus.COLLECTED)
                .build()
            );

            transferItemDAO.save(TransferItem.builder()
                .datasetPid("doi:10.5072/FK2/QZ0LJQ")
                .datasetVersion("3.0")
                .dveFilePath("src/test/resources/doi-10-5072-fk2-qz0ljqv-1-2/metadata/oai-ore.jsonld")
                .creationTime(OffsetDateTime.parse("2020-08-03T11:30:00Z"))
                .transferStatus(TransferItem.TransferStatus.TARRING)
                .build()
            );
        });

        var items = transferItemDAO.findByStatus(TransferItem.TransferStatus.METADATA_EXTRACTED);

        assertThat(items).extracting("datasetPid")
            .containsOnly("doi:10.5072/FK2/P4PHV7");
    }

}
