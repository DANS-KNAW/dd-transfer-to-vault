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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.stream.Collectors;

public class VaultCatalogServiceImpl implements VaultCatalogService {
    private static final Logger log = LoggerFactory.getLogger(VaultCatalogServiceImpl.class);

    private final String baseUrl;

    public VaultCatalogServiceImpl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    private TarApi getApi() {
        var api = new TarApi();
        api.setCustomBaseUrl(baseUrl);
        return api;
    }

    @Override
    public void addTar(Tar tar) throws ApiException {
        var api = getApi();
        api.addArchive(mapTarToAPI(tar));
    }

    @Override
    public void addOrUpdateTar(Tar tar) throws ApiException {
        var api = getApi();

        try {
            log.info("Checking if TAR is already sent to vault");
            var response = api.getArchiveByIdWithHttpInfo(tar.getTarUuid());

            log.info("Response did not throw an error, meaning the TAR exists in the vault; updating records");
            log.debug("API response code: {}", response.getStatusCode());
            api.updateArchive(tar.getTarUuid(), mapTarToAPI(tar));
        }
        catch (ApiException e) {
            if (e.getCode() == 404) {
                log.info("Archive not yet present in vault, creating");
                api.addArchive(mapTarToAPI(tar));
            }
            else {
                log.error("Received unexpected error from vault", e);
                throw e;
            }
        }

    }

    nl.knaw.dans.ttv.openapi.api.Tar mapTarToAPI(Tar tar) {
        var apiTar = new nl.knaw.dans.ttv.openapi.api.Tar();

        apiTar.setTarUuid(tar.getTarUuid());
        apiTar.setStagedDate(tar.getCreated().atOffset(ZoneOffset.UTC));

        if (tar.getDatetimeConfirmedArchived() != null) {
            apiTar.setArchivalDate(tar.getDatetimeConfirmedArchived().atOffset(ZoneOffset.UTC));
        }

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
