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

import lombok.AllArgsConstructor;
import nl.knaw.dans.ttv.core.InvalidTransferItemException;
import nl.knaw.dans.ttv.core.domain.FileContentAttributes;
import nl.knaw.dans.ttv.core.domain.FilenameAttributes;
import nl.knaw.dans.ttv.core.domain.FilesystemAttributes;
import nl.knaw.dans.ttv.core.oaiore.OaiOreMetadataReader;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Optional;

@AllArgsConstructor
public class TransferItemMetadataReaderImpl implements TransferItemMetadataReader {
    private final FileService fileService;
    private final OaiOreMetadataReader oaiOreMetadataReader;
    private final DataFileAttributesReader dataFileAttributesReader;

    @Override
    public FilenameAttributes getFilenameAttributes(Path path) throws InvalidTransferItemException {
        var dveFileName = new DveFileName(path.getFileName().toString());
        return FilenameAttributes.builder()
            .dveFilePath(path.toString())
            .dveFilename(path.getFileName().toString())
            .ocflObjectVersionNumber(dveFileName.getOcflObjectVersionNr())
            .build();
    }

    @Override
    public FilesystemAttributes getFilesystemAttributes(Path path) throws InvalidTransferItemException {

        try {
            var creationTime = fileService.getFilesystemAttribute(path, "creationTime");
            var time = Optional.ofNullable(creationTime)
                .map(FileTime.class::cast)
                .map(FileTime::toInstant)
                .map(t -> OffsetDateTime.ofInstant(t, ZoneId.systemDefault()))
                .orElse(null);

            var checksum = fileService.calculateChecksum(path);

            return new FilesystemAttributes(time, fileService.getFileSize(path), checksum);
        }
        catch (IOException e) {
            throw new InvalidTransferItemException(String.format("unable to read filesystem attributes for file %s", path.toString()), e);
        }
    }

    @Override
    public FileContentAttributes getFileContentAttributes(Path path) throws InvalidTransferItemException {

        try {
            var datasetVersionExport = fileService.openZipFile(path);

            var metadataContent = fileService.openFileFromZip(datasetVersionExport, Path.of("metadata/oai-ore.jsonld"));
            var oaiOre = IOUtils.toString(metadataContent, StandardCharsets.UTF_8);
            var fileContentAttributes = oaiOreMetadataReader.readMetadata(oaiOre);
            fileContentAttributes.setMetadata(oaiOre);

            var dataFileAttributes = dataFileAttributesReader.readDataFileAttributes(path);
            fileContentAttributes.setDataFileAttributes(dataFileAttributes);

            return fileContentAttributes;
        }
        catch (IOException e) {
            throw new InvalidTransferItemException(String.format("unable to read zip file contents for file '%s'", path), e);
        }
        catch (NullPointerException e) {
            throw new InvalidTransferItemException(String.format("unable to extract metadata from file '%s'", path), e);
        }
    }

    @Override
    public Optional<Path> getAssociatedXmlFile(Path path) {
        return Optional.empty();
    }
}
