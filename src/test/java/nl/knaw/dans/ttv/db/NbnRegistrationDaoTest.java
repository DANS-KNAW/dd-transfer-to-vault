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
import nl.knaw.dans.ttv.core.NbnRegistration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
public class NbnRegistrationDaoTest {

    private final DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
        .addEntityClass(NbnRegistration.class)
        .build();

    private NbnRegistrationDao nbnRegistrationDao;

    @BeforeEach
    public void setUp() {
        nbnRegistrationDao = new NbnRegistrationDao(daoTestRule.getSessionFactory());
    }

    @Test
    public void builder_should_set_correct_defaults() throws Exception {
        var nbn = "urn:nbn:nl:ui:13-123456";
        var location = new URI("http://localhost:20120/some-path");
        var r = NbnRegistration.builder()
            .nbn(nbn)
            .location(location)
            .build();
        assertThat(r.getTimestamp()).isNotNull();
        assertThat(r.getTimestampMessage()).isEqualTo("Registration scheduled");
        assertThat(r.getStatus()).isEqualTo(NbnRegistration.Status.PENDING);
    }

    @Test
    public void save_should_persist_entity() throws Exception {
        var nbn = "urn:nbn:nl:ui:13-123456";
        var location = new URI("http://localhost:20120/some-path");
        var r = NbnRegistration.builder()
            .nbn(nbn)
            .location(location)
            .build();
        var saved = daoTestRule.inTransaction(() -> nbnRegistrationDao.save(r));
        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getNbn()).isEqualTo(nbn);
        assertThat(saved.getLocation()).isEqualTo(location);
        assertThat(saved.getTimestamp()).isNotNull();
        assertThat(saved.getTimestampMessage()).isEqualTo("Registration scheduled");
        assertThat(saved.getStatus()).isEqualTo(NbnRegistration.Status.PENDING);
    }

    @Test
    public void getPendingRegistrations_should_return_only_pending_registrations() throws Exception {
        var nbn1 = "urn:nbn:nl:ui:13-123456";
        var location1 = new URI("http://localhost:20120/some-path");
        var r1 = NbnRegistration.builder()
            .nbn(nbn1)
            .location(location1)
            .status(NbnRegistration.Status.PENDING)
            .build();
        var nbn2 = "urn:nbn:nl:ui:13-123457";
        var location2 = new URI("http://localhost:20120/some-other-path");
        var r2 = NbnRegistration.builder()
            .nbn(nbn2)
            .location(location2)
            .status(NbnRegistration.Status.REGISTERED)
            .build();
        daoTestRule.inTransaction(() -> {
            nbnRegistrationDao.save(r1);
            nbnRegistrationDao.save(r2);
        });
        var pendingRegistrations = nbnRegistrationDao.getPendingRegistrations();
        assertThat(pendingRegistrations).hasSize(1);
        assertThat(pendingRegistrations.get(0).getNbn()).isEqualTo(nbn1);
    }

    @Test
    public void getFailedRegistrations_should_return_only_failed_registrations() throws Exception {
        var nbn1 = "urn:nbn:nl:ui:13-123456";
        var location1 = new URI("http://localhost:20120/some-path");
        var r1 = NbnRegistration.builder()
            .nbn(nbn1)
            .location(location1)
            .status(NbnRegistration.Status.FAILED)
            .build();
        var nbn2 = "urn:nbn:nl:ui:13-123457";
        var location2 = new URI("http://localhost:20120/some-other-path");
        var r2 = NbnRegistration.builder()
            .nbn(nbn2)
            .location(location2)
            .status(NbnRegistration.Status.REGISTERED)
            .build();
        daoTestRule.inTransaction(() -> {
            nbnRegistrationDao.save(r1);
            nbnRegistrationDao.save(r2);
        });
        var failedRegistrations = nbnRegistrationDao.getFailedRegistrations();
        assertThat(failedRegistrations).hasSize(1);
        assertThat(failedRegistrations.get(0).getNbn()).isEqualTo(nbn1);
    }

    @Test
    public void save_should_update_existing_entity() throws Exception {
        // Given
        var nbn = "urn:nbn:nl:ui:13-123456";
        var location = new URI("http://localhost:20120/some-path");
        var r = NbnRegistration.builder()
            .nbn(nbn)
            .location(location)
            .build();
        var saved = daoTestRule.inTransaction(() -> nbnRegistrationDao.save(r));
        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getNbn()).isEqualTo(nbn);
        assertThat(saved.getLocation()).isEqualTo(location);
        assertThat(saved.getTimestamp()).isNotNull();
        assertThat(saved.getTimestampMessage()).isEqualTo("Registration scheduled");
        assertThat(saved.getStatus()).isEqualTo(NbnRegistration.Status.PENDING);

        // When
        saved.setStatus(NbnRegistration.Status.REGISTERED);
        saved.setTimestampMessage("Registration completed");
        var updated = daoTestRule.inTransaction(() -> nbnRegistrationDao.save(saved));

        // Then
        assertThat(updated.getId()).isEqualTo(saved.getId());
        assertThat(updated.getNbn()).isEqualTo(nbn);
        assertThat(updated.getLocation()).isEqualTo(location);
        assertThat(updated.getTimestamp()).isNotNull();

        assertThat(updated.getTimestampMessage()).isEqualTo("Registration completed");
        assertThat(updated.getStatus()).isEqualTo(NbnRegistration.Status.REGISTERED);
    }
}
