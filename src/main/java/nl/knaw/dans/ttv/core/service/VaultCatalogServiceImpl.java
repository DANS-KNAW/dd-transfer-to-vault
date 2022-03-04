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
import nl.knaw.dans.ttv.openapi.api.TarPart;
import nl.knaw.dans.ttv.openapi.api.TransferItem;
import nl.knaw.dans.ttv.openapi.client.TarApi;

import java.nio.charset.StandardCharsets;
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

        apiTar.setTransferItems(tar.getTransferItems().stream().map(t -> {
            var transferItem = new TransferItem();
            transferItem.setBagId(t.getBagId());
            transferItem.setObjectVersion("1");
            transferItem.setDatastation(t.getDatasetDvInstance());
            transferItem.setDataversePid(t.getDatasetPid());
            transferItem.setDataversePidVersion(String.format("V%s.%s", t.getVersionMajor(), t.getVersionMinor()));
            transferItem.setNbn(t.getNbn());
            transferItem.setOtherId(t.getOtherId());
            transferItem.setOtherIdVersion(t.getOtherIdVersion());
            transferItem.setSwordClient(t.getSwordClient());
            transferItem.setSwordToken(t.getSwordToken());
            transferItem.setOcflObjectPath(t.getAipTarEntryName());
            transferItem.setFilepidToLocalPath(new String(t.getPidMapping(), StandardCharsets.UTF_8));
            transferItem.setMetadata(new String(t.getOaiOre(), StandardCharsets.UTF_8));

            return transferItem;
        }).collect(Collectors.toList()));

        return apiTar;
    }
}
