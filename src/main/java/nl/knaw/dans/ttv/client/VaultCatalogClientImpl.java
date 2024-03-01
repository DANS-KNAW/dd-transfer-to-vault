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

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ttv.client.mappers.OcflObjectVersionMapper;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.vaultcatalog.client.api.OcflObjectVersionDto;
import nl.knaw.dans.vaultcatalog.client.invoker.ApiException;
import nl.knaw.dans.vaultcatalog.client.resources.OcflObjectVersionApi;

import java.io.IOException;
import java.util.Comparator;

@Slf4j
@AllArgsConstructor
public class VaultCatalogClientImpl implements VaultCatalogClient {
    private final OcflObjectVersionApi ocflObjectVersionApi;

    @Override
    public void registerOcflObjectVersion(TransferItem transferItem) throws IOException {
        try {
            var newVersion = getObjectVersion(transferItem);
            var dto = OcflObjectVersionMapper.INSTANCE.mapParameters(transferItem);

            log.debug("Registering OCFL object version: {}", dto);

            var response = ocflObjectVersionApi.createOcflObjectVersion(transferItem.getBagId(), newVersion, dto);
            log.debug("Response: {}", response);

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
}
