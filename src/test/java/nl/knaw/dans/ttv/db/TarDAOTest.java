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
class TarDAOTest {
    private static final Logger log = LoggerFactory.getLogger(TarDAOTest.class);

    private final DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
        .addEntityClass(TransferItem.class)
        .addEntityClass(Tar.class)
        .addEntityClass(TarPart.class)
        .build();

    private TarDAO tarDAO;

    @BeforeEach
    void setUp() {
        tarDAO = new TarDAO(daoTestRule.getSessionFactory());
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

}
