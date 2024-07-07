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
import nl.knaw.dans.ttv.core.domain.FileContentAttributes;
import nl.knaw.dans.ttv.core.domain.FilenameAttributes;
import nl.knaw.dans.ttv.core.domain.FilesystemAttributes;
import nl.knaw.dans.vaultcatalog.client.api.DatasetDto;
import nl.knaw.dans.vaultcatalog.client.api.FileMetaDto;
import nl.knaw.dans.vaultcatalog.client.api.VersionExportDto;
import nl.knaw.dans.vaultcatalog.client.invoker.ApiException;
import nl.knaw.dans.vaultcatalog.client.resources.DefaultApi;
import org.mapstruct.factory.Mappers;

import java.io.IOException;

@Slf4j
@AllArgsConstructor
public class VaultCatalogClientImpl implements VaultCatalogClient {
    private final DefaultApi catalogApi;

    @Override
    public void registerOcflObjectVersion(FileContentAttributes fileContentAttributes, FilesystemAttributes filesystemAttributes, FilenameAttributes filenameAttributes) throws IOException {
        try {
            var datasetDto = getDataset(fileContentAttributes.getNbn());
            if (datasetDto == null) {
                if (filenameAttributes.getOcflObjectVersionNumber() != 1) {
                    throw new IllegalArgumentException("The OCFL object version number must be 1 for a new dataset.");
                }
                registerNewDataset(fileContentAttributes, filesystemAttributes);
            }
            else {
                updateExistingDataset(fileContentAttributes, filenameAttributes.getOcflObjectVersionNumber());
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

    private void registerNewDataset(FileContentAttributes fileContentAttributes, FilesystemAttributes filesystemAttributes) throws ApiException {
        var datasetDto = new DatasetDto()
            .nbn(fileContentAttributes.getNbn())
            .dataversePid(fileContentAttributes.getDataversePid())
            .swordToken(fileContentAttributes.getSwordToken())
            .dataSupplier(fileContentAttributes.getDataSupplier())
            .datastation(fileContentAttributes.getDatastation());

        var versionExportDto = new VersionExportDto()
            .bagId(fileContentAttributes.getBagId())
            .title(fileContentAttributes.getTitle())
            .ocflObjectVersionNumber(1)
            .skeletonRecord(false)
            .createdTimestamp(filesystemAttributes.getCreationTime())
            .dataversePidVersion(fileContentAttributes.getDataversePidVersion())
            .metadata(fileContentAttributes.getMetadata());

        for (var dataFile : fileContentAttributes.getDataFileAttributes()) {
            var dataFileDto = new FileMetaDto()
                .filepath(dataFile.getFilepath().toString())
                .fileUri(dataFile.getUri())
                .byteSize(dataFile.getSize())
                .sha1sum(dataFile.getSha1Checksum());
            versionExportDto.addFileMetasItem(dataFileDto);
        }
        datasetDto.addVersionExportsItem(versionExportDto);
        versionExportDto.setDatasetNbn(datasetDto.getNbn());
        catalogApi.addDataset(datasetDto.getNbn(), datasetDto);
    }

    private void updateExistingDataset(FileContentAttributes fileContentAttributes, int ocflObjectVersion) throws ApiException {
        var dveDto = catalogApi.getVersionExport(fileContentAttributes.getNbn(), ocflObjectVersion);
        if (dveDto != null) {
            if (Boolean.FALSE.equals(dveDto.getSkeletonRecord())) {
                throw new IllegalArgumentException("The Dataset Version Export record cannot be updated because it is not a skeleton record.");
            }
            dveDto.setBagId(fileContentAttributes.getBagId());
            dveDto.setDatasetNbn(fileContentAttributes.getNbn());
            dveDto.setDataversePidVersion(fileContentAttributes.getDataversePidVersion());
            dveDto.setOtherId(fileContentAttributes.getOtherId());
            dveDto.setOtherIdVersion(fileContentAttributes.getOtherIdVersion());
            dveDto.setMetadata(fileContentAttributes.getMetadata());
            dveDto.setSkeletonRecord(false);
            dveDto.setTitle(fileContentAttributes.getTitle());

            dveDto.setFileMetas(null); // clear existing fileMetas
            for (var dataFile : fileContentAttributes.getDataFileAttributes()) {
                var dataFileDto = new FileMetaDto()
                    .filepath(dataFile.getFilepath().toString())
                    .fileUri(dataFile.getUri())
                    .byteSize(dataFile.getSize())
                    .sha1sum(dataFile.getSha1Checksum());
                dveDto.addFileMetasItem(dataFileDto);
            }
        }
        catalogApi.setVersionExport(dveDto.getDatasetNbn(), dveDto.getOcflObjectVersionNumber(), dveDto);
    }
}
