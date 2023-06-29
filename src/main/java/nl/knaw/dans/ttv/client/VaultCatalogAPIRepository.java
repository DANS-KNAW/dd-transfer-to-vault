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


import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ttv.api.ApiException;
import nl.knaw.dans.ttv.api.OcflObjectVersionDto;
import nl.knaw.dans.ttv.client.mappers.OcflObjectVersionMapper;
import nl.knaw.dans.ttv.core.VaultCatalogRepository;
import nl.knaw.dans.ttv.db.Tar;
import nl.knaw.dans.ttv.db.TransferItem;
import nl.knaw.dans.ttv.resource.OcflObjectVersionApi;
import nl.knaw.dans.ttv.resource.TarApi;

import java.io.IOException;
import java.util.Comparator;

@Slf4j
public class VaultCatalogAPIRepository implements VaultCatalogRepository {

    private final TarApi tarApi;
    private final OcflObjectVersionApi ocflObjectVersionApi;

    public VaultCatalogAPIRepository(String baseUrl) {
        this.tarApi = new TarApi();
        this.tarApi.setCustomBaseUrl(baseUrl);

        this.ocflObjectVersionApi = new OcflObjectVersionApi();
        this.ocflObjectVersionApi.setCustomBaseUrl(baseUrl);
    }

    @Override
    public void registerOcflObjectVersion(TransferItem transferItem) throws IOException {
        try {
            var newVersion = getObjectVersion(transferItem);
            var dto = OcflObjectVersionMapper.INSTANCE.mapParameters(transferItem);

            log.info("Registering OCFL object version: {}", dto);

            var response = ocflObjectVersionApi.createOcflObjectVersion(transferItem.getBagId(), newVersion, dto);
            log.info("Response: {}", response);

            transferItem.setOcflObjectVersion(newVersion);
        }
        catch (ApiException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private Integer getObjectVersion(TransferItem transferItem) throws IOException {
        try {
            // if transfer item already has an object version (it is not null), use that
            if (transferItem.getOcflObjectVersion() != null) {
                return transferItem.getOcflObjectVersion();
            }

            // if transfer item is coming from the dd-vault-ingest-flow, use the latest version
            // this is indicated by the skeletonRecord property; if set to true, use that version
            // otherwise, create a new version for this bag id (n+1 if one already exists)
            var items = ocflObjectVersionApi.getOcflObjectsByBagId(transferItem.getBagId());

            var latestVersion = items.stream()
                .max(Comparator.comparingInt(OcflObjectVersionDto::getObjectVersion))
                .orElse(null);

            var newVersion = 1;

            if (latestVersion != null) {
                if (latestVersion.getSkeletonRecord()) {
                    newVersion = latestVersion.getObjectVersion();
                }
                else {
                    newVersion = latestVersion.getObjectVersion() + 1;
                }
            }

            return newVersion;
        }
        catch (ApiException e) {
            throw new IOException(e.getMessage(), e);
        }
    }


    @Override
    public void registerTar(Tar tar) throws IOException {

        // creates a tar with some ocfl objects

    }

//    @Override
//    public void addTar(Tar tar) throws ApiException {
//        var api = getApi();
//        api.addArchive(mapTarToAPI(tar));
//    }
//
//    @Override
//    public void addOrUpdateTar(Tar tar) throws ApiException {
//        var api = getApi();
//
//        try {
//            log.info("Checking if TAR is already sent to vault");
//            var response = api.getArchiveByIdWithHttpInfo(tar.getTarUuid());
//
//            log.info("Response did not throw an error, meaning the TAR exists in the vault; updating records");
//            log.debug("API response code: {}", response.getStatusCode());
//            api.updateArchive(tar.getTarUuid(), mapTarToAPI(tar));
//        }
//        catch (ApiException e) {
//            if (e.getCode() == 404) {
//                log.info("Archive not yet present in vault, creating");
//                api.addArchive(mapTarToAPI(tar));
//            }
//            else {
//                log.error("Received unexpected error from vault", e);
//                throw e;
//            }
//        }
//
//    }
//
//    TarDto mapTarToAPI(Tar tar) {
//        var apiTar = new TarDto();
//
//        apiTar.setTarUuid(tar.getTarUuid());
//        apiTar.setStagedDate(tar.getCreated().atOffset(ZoneOffset.UTC));
//
//        if (tar.getDatetimeConfirmedArchived() != null) {
//            apiTar.setArchivalDate(tar.getDatetimeConfirmedArchived().atOffset(ZoneOffset.UTC));
//        }
//
//        apiTar.setVaultPath(tar.getVaultPath());
//        apiTar.setTarParts(tar.getTarParts().stream().map(p -> {
//            var part = new TarPartDto();
//            part.setPartName(p.getPartName());
//            part.setChecksumAlgorithm(p.getChecksumAlgorithm());
//            part.setChecksumValue(p.getChecksumValue());
//
//            return part;
//        }).collect(Collectors.toList()));
//
//        apiTar.setOcflObjects(tar.getTransferItems().stream().map(t -> {
//            var ocflObject = new OcflObject();
//            ocflObject.setBagId(t.getBagId());
//            ocflObject.setObjectVersion("1");
//            ocflObject.setDatastation(t.getDatasetDvInstance());
//            ocflObject.setDataversePid(t.getDatasetPid());
//            ocflObject.setDataversePidVersion(String.format("V%s.%s", t.getVersionMajor(), t.getVersionMinor()));
//            ocflObject.setNbn(t.getNbn());
//            ocflObject.setOtherId(t.getOtherId());
//            ocflObject.setOtherIdVersion(t.getOtherIdVersion());
//            ocflObject.setSwordClient(t.getSwordClient());
//            ocflObject.setSwordToken(t.getSwordToken());
//            ocflObject.setOcflObjectPath(t.getAipTarEntryName());
//            ocflObject.setFilepidToLocalPath(new String(t.getPidMapping(), StandardCharsets.UTF_8));
//            ocflObject.setMetadata(new String(t.getOaiOre(), StandardCharsets.UTF_8));
//            ocflObject.setVersionMajor(t.getVersionMajor());
//            ocflObject.setVersionMinor(t.getVersionMinor());
//            ocflObject.setExportTimestamp(t.getBagDepositDate().atOffset(ZoneOffset.UTC));
//
//            return ocflObject;
//        }).collect(Collectors.toList()));
//
//        return apiTar;
//    }

}
