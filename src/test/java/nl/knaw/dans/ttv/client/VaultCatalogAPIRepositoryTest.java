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
package nl.knaw.dans.ttv.client;

import nl.knaw.dans.ttv.api.OcflObjectVersionDto;
import nl.knaw.dans.ttv.api.OcflObjectVersionParametersDto;
import nl.knaw.dans.ttv.db.TransferItem;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VaultCatalogAPIRepositoryTest {

    @Test
    void registerOcflObjectVersion_should_set_version_to_1_if_no_records_exist() throws Exception {
        var tarApi = Mockito.mock(TarApi.class);
        var ocflObjectVersionApi = Mockito.mock(OcflObjectVersionApi.class);
        var repo = new VaultCatalogAPIRepository(tarApi, ocflObjectVersionApi);

        var transferItem = TransferItem.builder()
            .bagId("urn:uuid:" + UUID.randomUUID())
            .build();

        repo.registerOcflObjectVersion(transferItem);

        assertEquals(1, transferItem.getOcflObjectVersion());
    }

    @Test
    void registerOcflObjectVersion_should_set_version_to_2_if_one_record_exists() throws Exception {
        var tarApi = Mockito.mock(TarApi.class);
        var ocflObjectVersionApi = Mockito.mock(OcflObjectVersionApi.class);
        var repo = new VaultCatalogAPIRepository(tarApi, ocflObjectVersionApi);

        var transferItem = TransferItem.builder()
            .bagId("urn:uuid:" + UUID.randomUUID())
            .build();

        Mockito.when(ocflObjectVersionApi.getOcflObjectsByBagId(Mockito.eq(transferItem.getBagId())))
            .thenReturn(List.of(
                new OcflObjectVersionDto()
                    .objectVersion(1)
                    .skeletonRecord(false)
            ));

        repo.registerOcflObjectVersion(transferItem);

        assertEquals(2, transferItem.getOcflObjectVersion());
    }

    @Test
    void registerOcflObjectVersion_should_set_version_to_3_if_one_record_exists_but_it_is_a_SkeletonRecord() throws Exception {
        var tarApi = Mockito.mock(TarApi.class);
        var ocflObjectVersionApi = Mockito.mock(OcflObjectVersionApi.class);
        var repo = new VaultCatalogAPIRepository(tarApi, ocflObjectVersionApi);

        var transferItem = TransferItem.builder()
            .bagId("urn:uuid:" + UUID.randomUUID())
            .build();

        Mockito.when(ocflObjectVersionApi.getOcflObjectsByBagId(Mockito.eq(transferItem.getBagId())))
            .thenReturn(List.of(
                new OcflObjectVersionDto()
                    .objectVersion(3)
                    .skeletonRecord(true)
            ));

        repo.registerOcflObjectVersion(transferItem);

        assertEquals(3, transferItem.getOcflObjectVersion());
    }

    @Test
    void registerOcflObjectVersion_should_not_change_version_if_already_set() throws Exception {
        var tarApi = Mockito.mock(TarApi.class);
        var ocflObjectVersionApi = Mockito.mock(OcflObjectVersionApi.class);
        var repo = new VaultCatalogAPIRepository(tarApi, ocflObjectVersionApi);

        var transferItem = TransferItem.builder()
            .bagId("urn:uuid:" + UUID.randomUUID())
            .ocflObjectVersion(1)
            .build();

        Mockito.when(ocflObjectVersionApi.getOcflObjectsByBagId(Mockito.eq(transferItem.getBagId())))
            .thenReturn(List.of(
                new OcflObjectVersionDto()
                    .objectVersion(3)
                    .skeletonRecord(true)
            ));

        repo.registerOcflObjectVersion(transferItem);

        assertEquals(1, transferItem.getOcflObjectVersion());
    }

    @Test
    void registerOcflObjectVersion_should_fully_map_TransferItem_to_OcflObjectVersionDto() throws Exception {
        var tarApi = Mockito.mock(TarApi.class);
        var ocflObjectVersionApi = Mockito.mock(OcflObjectVersionApi.class);
        var repo = new VaultCatalogAPIRepository(tarApi, ocflObjectVersionApi);

        var transferItem = TransferItem.builder()
            .bagId("urn:uuid:" + UUID.randomUUID())
            .ocflObjectVersion(2)
            .dataversePid("datasetPid")
            .dataversePidVersion("2.1")
            .creationTime(OffsetDateTime.now().minus(1, ChronoUnit.DAYS))
            .dveFilePath("dveFilePath")
            .nbn("nbn")
            .otherId("otherId")
            .otherIdVersion("otherIdVersion")
            .dataSupplier("swordClient")
            .swordToken("swordToken")
            .datastation("datasetDvInstance")
            .ocflObjectPath("ab/cd/12/34-test")
            .bagSha256Checksum("bagChecksum")
            .bagSize(123L)
            .transferStatus(TransferItem.TransferStatus.COLLECTED)
            .metadata("{}")
            .filepidToLocalPath("a  b")
            .build();

        repo.registerOcflObjectVersion(transferItem);

        var argumentCaptor = ArgumentCaptor.forClass(OcflObjectVersionParametersDto.class);

        // get argument passed to mock
        Mockito.verify(ocflObjectVersionApi).createOcflObjectVersion(
            Mockito.eq(transferItem.getBagId()),
            Mockito.eq(2),
            argumentCaptor.capture()
        );

        var expected = new OcflObjectVersionParametersDto()
            .swordToken("swordToken")
            .nbn("nbn")
            .datastation("datasetDvInstance")
            .dataSupplier("swordClient")
            .dataversePid("datasetPid")
            .dataversePidVersion("2.1")
            .otherId("otherId")
            .otherIdVersion("otherIdVersion")
            .ocflObjectPath("ab/cd/12/34-test")
            .metadata(Map.of())
            .filepidToLocalPath("a  b")
            .skeletonRecord(false);

        assertEquals(expected, argumentCaptor.getValue());
    }
}