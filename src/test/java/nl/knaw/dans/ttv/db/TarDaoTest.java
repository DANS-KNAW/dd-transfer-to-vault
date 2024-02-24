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
import nl.knaw.dans.ttv.core.Tar;
import nl.knaw.dans.ttv.core.TarPart;
import nl.knaw.dans.ttv.core.TransferItem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
class TarDaoTest {
    private static final Logger log = LoggerFactory.getLogger(TarDaoTest.class);

    private final DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
        .addEntityClass(TransferItem.class)
        .addEntityClass(Tar.class)
        .addEntityClass(TarPart.class)
        .build();

    private TarDao tarDAO;

    @BeforeEach
    void setUp() {
        tarDAO = new TarDao(daoTestRule.getSessionFactory());
    }

    @Test
    void findAllTarsToBeConfirmed() {
        daoTestRule.inTransaction(() -> {
            tarDAO.save(new Tar("uuid1", Tar.TarStatus.OCFLTARCREATED, false));
            tarDAO.save(new Tar("uuid2", Tar.TarStatus.OCFLTARCREATED, true));
            tarDAO.save(new Tar("uuid3", Tar.TarStatus.TARRING, false));
            tarDAO.save(new Tar("uuid4", Tar.TarStatus.OCFLTARCREATED, false));
        });

        var items = tarDAO.findAllTarsToBeConfirmed();
        assertThat(items).extracting("tarUuid").containsOnly("uuid1", "uuid4");
    }

    @Test
    void testNoDuplicateTarParts() {
        daoTestRule.inTransaction(() -> {
            var tar = new Tar("uuid1", Tar.TarStatus.OCFLTARCREATED, false);

            var parts = List.of(
                new TarPart("0000", "md5", "checksum", tar),
                new TarPart("0001", "md5", "checksum2", tar)
            );

            tar.setTarParts(parts);
            tarDAO.save(tar);
            tarDAO.evict(tar);
        });

        daoTestRule.inTransaction(() -> {
            var tar = tarDAO.findById("uuid1").get();
            var parts = List.of(
                new TarPart("0000", "md5", "checksum3", tar)
            );
            tarDAO.saveWithParts(tar, parts);
        });

        var parts = tarDAO.findAllParts();
        Assertions.assertEquals(1, parts.size());
    }

    @Test
    void findByStatus() {
        daoTestRule.inTransaction(() -> {
            tarDAO.save(new Tar("uuid1", Tar.TarStatus.OCFLTARCREATED, false));
            tarDAO.save(new Tar("uuid2", Tar.TarStatus.CONFIRMEDARCHIVED, true));
            tarDAO.save(new Tar("uuid3", Tar.TarStatus.TARRING, false));
            tarDAO.save(new Tar("uuid4", Tar.TarStatus.OCFLTARFAILED, false));
        });

        assertThat(tarDAO.findByStatus(Tar.TarStatus.OCFLTARCREATED)).extracting("tarUuid").containsOnly("uuid1");
        assertThat(tarDAO.findByStatus(Tar.TarStatus.CONFIRMEDARCHIVED)).extracting("tarUuid").containsOnly("uuid2");
        assertThat(tarDAO.findByStatus(Tar.TarStatus.TARRING)).extracting("tarUuid").containsOnly("uuid3");
        assertThat(tarDAO.findByStatus(Tar.TarStatus.OCFLTARFAILED)).extracting("tarUuid").containsOnly("uuid4");
    }
}
