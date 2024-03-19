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
import java.util.regex.Pattern;

public class TransferItemMetadataReaderImpl implements TransferItemMetadataReader {
    private static final Pattern DATAVERSE_PATTERN = Pattern.compile(
        "(?<doi>doi-10-[0-9]{4,}-[A-Za-z0-9]{2,}-[A-Za-z0-9]{6})-?" +
            "(?<schema>datacite)?.?" +
            "v(?<major>[0-9]+).(?<minor>[0-9]+)" +
            "(\\.zip)"
    );

    private static final Pattern VAAS_PATTERN = Pattern.compile("^vaas-[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{12}-v\\d+(\\.zip)$");
    Pattern DVE_NAME_PATTERN = Pattern.compile("(?<identifier>.*?)(-v(?<ocflobjectversionnr>\\d+))?(-ttv(?<internalid>\\d+))?\\.(?<extension>zip)");
    private final FileService fileService;
    private final OaiOreMetadataReader oaiOreMetadataReader;

    public TransferItemMetadataReaderImpl(FileService fileService, OaiOreMetadataReader oaiOreMetadataReader) {
        this.fileService = fileService;
        this.oaiOreMetadataReader = oaiOreMetadataReader;
    }

    @Override
    public FilenameAttributes getFilenameAttributes(Path path) throws InvalidTransferItemException {
        var normalizedFilename = normalizeFilename(path.getFileName().toString());
        var internalId = getInternalId(path.getFileName().toString());
        var ocflObjectVersionNumber = getOcflObjectVersionNumber(path.getFileName().toString());
        return FilenameAttributes.builder()
            .dveFilePath(path.toString())
            .dveFilename(normalizedFilename)
            .internalId(internalId)
            .ocflObjectVersionNumber(ocflObjectVersionNumber)
            .build();
    }

    /**
     * Input can be a canonical filename or original DVE file name. The output is the original DVE filename.
     *
     * @param filename the filename to normalize
     * @return the normalized filename
     */
    private String normalizeFilename(String filename) {
        var dveNameMatcher = DVE_NAME_PATTERN.matcher(filename);

        if (dveNameMatcher.matches()) {
            var identifier = dveNameMatcher.group("identifier");
            var ocflObjectVersionNr = dveNameMatcher.group("ocflobjectversionnr");
            var extension = dveNameMatcher.group("extension");
            if (ocflObjectVersionNr != null) {
                return String.format("%s-v%s.%s", identifier, ocflObjectVersionNr, extension);
            }
            else {
                return String.format("%s.%s", identifier, extension);
            }
        }
        throw new IllegalArgumentException("filename does not match expected pattern(s)");
    }

    private Long getInternalId(String filename) {
        var dveNameMatcher = DVE_NAME_PATTERN.matcher(filename);

        if (dveNameMatcher.matches()) {
            var internalId = dveNameMatcher.group("internalid");
            return internalId != null ? Long.parseLong(internalId) : null;
        }

        throw new IllegalArgumentException("filename does not match expected pattern(s)");
    }

    private Integer getOcflObjectVersionNumber(String filename) {
        var dveNameMatcher = DVE_NAME_PATTERN.matcher(filename);

        if (dveNameMatcher.matches()) {
            var ocflObjectVersionNr = dveNameMatcher.group("ocflobjectversionnr");
            return ocflObjectVersionNr != null ? Integer.parseInt(ocflObjectVersionNr) : null;
        }

        throw new IllegalArgumentException("filename does not match expected pattern(s)");
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
            var pidMappingContent = fileService.openFileFromZip(datasetVersionExport, Path.of("metadata/pid-mapping.txt"));

            var oaiOre = IOUtils.toString(metadataContent, StandardCharsets.UTF_8);
            var pidMapping = IOUtils.toString(pidMappingContent, StandardCharsets.UTF_8);

            var fileContentAttributes = oaiOreMetadataReader.readMetadata(oaiOre);

            fileContentAttributes.setMetadata(oaiOre);
            fileContentAttributes.setFilepidToLocalPath(pidMapping);

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
        var filename = normalizeFilename(path.getFileName().toString());
        var matcher = DATAVERSE_PATTERN.matcher(filename);
        var xml = matcher.matches()
            ? matcher.group("doi") + "-datacite.v" + matcher.group("major") + "." + matcher.group("minor") + ".xml"
            : null;

        if (xml != null) {
            return Optional.of(path.getParent().resolve(xml));
        }

        return Optional.empty();
    }
}
