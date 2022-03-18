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

package nl.knaw.dans.ttv.core.service;

import nl.knaw.dans.ttv.db.Tar;
import nl.knaw.dans.ttv.openapi.ApiException;
import nl.knaw.dans.ttv.openapi.api.OcflObject;
import nl.knaw.dans.ttv.openapi.api.TarPart;
import nl.knaw.dans.ttv.openapi.client.TarApi;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.stream.Collectors;

public class VaultCatalogServiceImpl implements VaultCatalogService {
    private final String baseUrl;

    public VaultCatalogServiceImpl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    @Override
    public void addTar(Tar tar) throws ApiException {
        var api = new TarApi();
        api.setCustomBaseUrl(baseUrl);
        api.addArchive(mapTarToAPI(tar));
    }

    nl.knaw.dans.ttv.openapi.api.Tar mapTarToAPI(Tar tar) {
        var apiTar = new nl.knaw.dans.ttv.openapi.api.Tar();

        apiTar.setTarUuid(tar.getTarUuid());
        apiTar.setArchivalDate(tar.getDatetimeConfirmedArchived().atOffset(ZoneOffset.UTC));
        apiTar.setVaultPath(tar.getVaultPath());
        apiTar.setTarParts(tar.getTarParts().stream().map(p -> {
            var part = new TarPart();
            part.setPartName(p.getPartName());
            part.setChecksumAlgorithm(p.getChecksumAlgorithm());
            part.setChecksumValue(p.getChecksumValue());

            return part;
        }).collect(Collectors.toList()));

        apiTar.setOcflObjects(tar.getTransferItems().stream().map(t -> {
            var ocflObject = new OcflObject();
            ocflObject.setBagId(t.getBagId());
            ocflObject.setObjectVersion("1");
            ocflObject.setDatastation(t.getDatasetDvInstance());
            ocflObject.setDataversePid(t.getDatasetPid());
            ocflObject.setDataversePidVersion(String.format("V%s.%s", t.getVersionMajor(), t.getVersionMinor()));
            ocflObject.setNbn(t.getNbn());
            ocflObject.setOtherId(t.getOtherId());
            ocflObject.setOtherIdVersion(t.getOtherIdVersion());
            ocflObject.setSwordClient(t.getSwordClient());
            ocflObject.setSwordToken(t.getSwordToken());
            ocflObject.setOcflObjectPath(t.getAipTarEntryName());
            ocflObject.setFilepidToLocalPath(new String(t.getPidMapping(), StandardCharsets.UTF_8));
            ocflObject.setMetadata(new String(t.getOaiOre(), StandardCharsets.UTF_8));
            ocflObject.setVersionMajor(t.getVersionMajor());
            ocflObject.setVersionMinor(t.getVersionMinor());
            ocflObject.setExportTimestamp(t.getBagDepositDate().atOffset(ZoneOffset.UTC));

            return ocflObject;
        }).collect(Collectors.toList()));

        return apiTar;
    }
}
