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
import nl.knaw.dans.ttv.api.OcflObjectVersionDto;
import nl.knaw.dans.ttv.api.OcflObjectVersionRefDto;
import nl.knaw.dans.ttv.api.TarParameterDto;
import nl.knaw.dans.ttv.api.TarPartParameterDto;
import nl.knaw.dans.ttv.client.mappers.OcflObjectVersionMapper;
import nl.knaw.dans.ttv.core.VaultCatalogRepository;
import nl.knaw.dans.ttv.db.Tar;
import nl.knaw.dans.ttv.db.TransferItem;

import java.io.IOException;
import java.util.Comparator;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class VaultCatalogAPIRepository implements VaultCatalogRepository {

    private final TarApi tarApi;
    private final OcflObjectVersionApi ocflObjectVersionApi;

    public VaultCatalogAPIRepository(TarApi tarApi, OcflObjectVersionApi ocflObjectVersionApi) {
        this.tarApi = tarApi;
        this.ocflObjectVersionApi = ocflObjectVersionApi;
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
            throw new IOException(e.getResponseBody(), e);
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
            throw new IOException(e.getResponseBody(), e);
        }
    }

    @Override
    public void registerTar(Tar tar) throws IOException {
        try {
            var params = new TarParameterDto()
                .tarUuid(UUID.fromString(tar.getTarUuid()))
                .archivalDate(tar.getDatetimeConfirmedArchived())
                .vaultPath(tar.getVaultPath())
                .tarParts(tar.getTarParts().stream()
                    .map(p -> new TarPartParameterDto()
                        .partName(p.getPartName())
                        .checksumAlgorithm(p.getChecksumAlgorithm())
                        .checksumValue(p.getChecksumValue()))
                    .collect(Collectors.toList()))
                .ocflObjectVersions(tar.getTransferItems().stream()
                    .map(item -> new OcflObjectVersionRefDto()
                        .bagId(item.getBagId())
                        .objectVersion(item.getOcflObjectVersion()))
                    .collect(Collectors.toList()));

            log.info("Registering TAR: {}", params);

            // check if tar already exists
            try {
                var existing = tarApi.getArchiveByIdWithHttpInfo(UUID.fromString(tar.getTarUuid()));
                log.debug("Response code for tar with UUID {}: {}", tar.getTarUuid(), existing.getStatusCode());

                log.info("Tar with UUID {} already exists in vault", tar.getTarUuid());
                tarApi.updateArchive(UUID.fromString(tar.getTarUuid()), params);
            }
            catch (ApiException e) {
                if (e.getCode() == 404) {
                    log.info("Tar with UUID {} does not exist in vault; adding it", tar.getTarUuid());
                    tarApi.addArchive(params);
                }
                else {
                    throw e;
                }
            }
        }
        catch (ApiException e) {
            throw new IOException(e.getResponseBody(), e);
        }
    }
}
