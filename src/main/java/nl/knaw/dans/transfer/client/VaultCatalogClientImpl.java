/*
 * Copyright (C) 2025 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.transfer.client;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.transfer.core.DveMetadata;
import nl.knaw.dans.vaultcatalog.client.api.DatasetDto;
import nl.knaw.dans.vaultcatalog.client.api.FileMetaDto;
import nl.knaw.dans.vaultcatalog.client.api.VersionExportDto;
import nl.knaw.dans.vaultcatalog.client.invoker.ApiException;
import nl.knaw.dans.vaultcatalog.client.resources.DefaultApi;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;

@Slf4j
@AllArgsConstructor
public class VaultCatalogClientImpl implements VaultCatalogClient {
    private final DefaultApi catalogApi;

    @Override
    public int registerOcflObjectVersion(String datastation, DveMetadata dveMetadata, int ocflObjectVersion) throws IOException {
        try {
            var datasetDto = getDataset(dveMetadata.getNbn());
            if (datasetDto == null) { // Data Stations only
                addNewDataset(datastation, dveMetadata);
                return 1;
            }
            else if (ocflObjectVersion == -1) { // Data Stations only
                return addNewVersionExport(datasetDto, dveMetadata);
            }
            else { // VaaS only
                updateExistingSkeletonVersionExport(datasetDto, dveMetadata, ocflObjectVersion);
                return ocflObjectVersion;
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

    private void addNewDataset(String datastation, DveMetadata dveMetadata) throws ApiException {
        var datasetDto = new DatasetDto()
            .nbn(dveMetadata.getNbn())
            .dataversePid(dveMetadata.getDataversePid())
            .swordToken(dveMetadata.getSwordToken())
            .dataSupplier(dveMetadata.getDataSupplier())
            .datastation(datastation);

        var dveDto = new VersionExportDto();
        dveDto.setOcflObjectVersionNumber(1);
        setVersionExportMetadata(dveMetadata, dveDto);
        setDataFilesOnVersionExport(dveMetadata, dveDto);
        datasetDto.addVersionExportsItem(dveDto);
        dveDto.setDatasetNbn(datasetDto.getNbn());
        catalogApi.addDataset(datasetDto.getNbn(), datasetDto);
    }

    private Path removeBaseFolder(Path path) {
        return path.subpath(1, path.getNameCount());
    }

    private void updateExistingSkeletonVersionExport(DatasetDto datasetDto, DveMetadata dveMetadata, int OcflObjectVersion) throws ApiException {
        assert datasetDto.getVersionExports() != null;
        var dveDto = datasetDto.getVersionExports()
            .stream()
            .max(Comparator.comparing(VersionExportDto::getOcflObjectVersionNumber))
            .orElseThrow(() -> new IllegalArgumentException("No version export found for the dataset"));

        if (Boolean.FALSE.equals(dveDto.getSkeletonRecord())) {
            throw new IllegalArgumentException("The Dataset Version Export record cannot be updated because it is not a skeleton record.");
        }
        setVersionExportMetadata(dveMetadata, dveDto);
        setDataFilesOnVersionExport(dveMetadata, dveDto);
        catalogApi.setVersionExport(dveDto.getDatasetNbn(), dveDto.getOcflObjectVersionNumber(), dveDto);
    }

    private int addNewVersionExport(DatasetDto datasetDto, DveMetadata dveMetadata) throws ApiException {
        var dveDto = new VersionExportDto();
        assert datasetDto.getVersionExports() != null;
        int ocflObjectVersion = datasetDto.getVersionExports().size() + 1;
        dveDto.setOcflObjectVersionNumber(ocflObjectVersion);
        setVersionExportMetadata(dveMetadata, dveDto);
        setDataFilesOnVersionExport(dveMetadata, dveDto);
        datasetDto.addVersionExportsItem(dveDto);
        catalogApi.setVersionExport(datasetDto.getNbn(), dveDto.getOcflObjectVersionNumber(), dveDto);
        return ocflObjectVersion;
    }

    private void setVersionExportMetadata(DveMetadata dveMetadata, VersionExportDto dveDto) {
        dveDto.setCreatedTimestamp(dveMetadata.getCreationTime());
        dveDto.setBagId(dveMetadata.getBagId());
        dveDto.setDatasetNbn(dveMetadata.getNbn());
        dveDto.setDataversePidVersion(dveMetadata.getDataversePidVersion());
        dveDto.setOtherId(dveMetadata.getOtherId());
        dveDto.setOtherIdVersion(dveMetadata.getOtherIdVersion());
        dveDto.setMetadata(dveMetadata.getMetadata());
        dveDto.setSkeletonRecord(false);
        dveDto.setTitle(dveMetadata.getTitle());
        dveDto.setExporter(dveMetadata.getExporter());
        dveDto.setExporterVersion(dveMetadata.getExporterVersion());
    }

    private void setDataFilesOnVersionExport(DveMetadata dveMetadata, VersionExportDto dveDto) {
        dveDto.setFileMetas(null); // clear existing fileMetas
        for (var dataFile : dveMetadata.getDataFileAttributes()) {
            var dataFileDto = new FileMetaDto()
                .filepath(removeBaseFolder(dataFile.getFilepath()).toString())
                .fileUri(dataFile.getUri())
                .byteSize(dataFile.getSize())
                .sha1sum(dataFile.getSha1Checksum());
            dveDto.addFileMetasItem(dataFileDto);
        }
    }

}
