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
import nl.knaw.dans.ttv.Conversions;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.vaultcatalog.client.api.DatasetDto;
import nl.knaw.dans.vaultcatalog.client.invoker.ApiException;
import nl.knaw.dans.vaultcatalog.client.resources.DefaultApi;
import org.mapstruct.factory.Mappers;

import java.io.IOException;
import java.util.List;

@Slf4j
@AllArgsConstructor
public class VaultCatalogClientImpl implements VaultCatalogClient {
    private static final Conversions conversions = Mappers.getMapper(Conversions.class);

    private final DefaultApi catalogApi;

    @Override
    public void registerOcflObjectVersion(TransferItem transferItem) throws IOException {
        try {
            var datasetDto = getDataset(transferItem.getNbn());
            if (datasetDto == null) {
                registerNewDataset(transferItem);
            }
            else {
                updateExistingDataset(transferItem);
            }
        }
        catch (ApiException e) {
            throw new IOException(e.getResponseBody(), e);
        }
    }

    private DatasetDto getDataset(String nbn) {
        try {
            return catalogApi.getDataset(nbn, null);
        }
        catch (ApiException e) {
            if (e.getCode() == 404) {
                return null;
            }
            throw new RuntimeException(e);
        }
    }

    private void registerNewDataset(TransferItem transferItem) throws ApiException {
        var datasetDto = conversions.mapTransferItemToDatasetDto(transferItem);
        var dveDto = conversions.mapTransferItemToVersionExportDto(transferItem);
        if (transferItem.getOcflObjectVersionNumber() != null && transferItem.getOcflObjectVersionNumber() != 1) {
            throw new IllegalArgumentException("The OCFL object version number must be 1 for a new dataset.");
        }
        dveDto.setOcflObjectVersionNumber(1);
        dveDto.setDatasetNbn(datasetDto.getNbn());
        datasetDto.setVersionExports(List.of(dveDto));
        catalogApi.addDataset(datasetDto.getNbn(), datasetDto);
    }

    private void updateExistingDataset(TransferItem transferItem) throws ApiException {
        var dveDto = catalogApi.getVersionExport(transferItem.getNbn(), transferItem.getOcflObjectVersionNumber());
        if (dveDto != null) {
            if (Boolean.FALSE.equals(dveDto.getSkeletonRecord())) {
                throw new IllegalArgumentException("The Dataset Version Export record cannot be updated because it is not a skeleton record.");
            }
            conversions.updateVersionExportDtoFromTransferItem(transferItem, dveDto);
        }
        catalogApi.setVersionExport(dveDto.getDatasetNbn(), dveDto.getOcflObjectVersionNumber(), dveDto);
    }
}
