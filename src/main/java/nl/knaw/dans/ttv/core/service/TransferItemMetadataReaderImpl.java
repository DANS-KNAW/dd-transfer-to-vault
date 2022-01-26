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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.knaw.dans.ttv.core.InvalidTransferItemException;
import nl.knaw.dans.ttv.core.dto.FileContentAttributes;
import nl.knaw.dans.ttv.core.dto.FilenameAttributes;
import nl.knaw.dans.ttv.core.dto.FilesystemAttributes;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipFile;

public class TransferItemMetadataReaderImpl implements TransferItemMetadataReader {
    private static final String DOI_PATTERN = "(?<doi>doi-10-[0-9]{4,}-[A-Za-z0-9]{2,}-[A-Za-z0-9]{6})-?";
    private static final String SCHEMA_PATTERN = "(?<schema>datacite)?.?";
    private static final String DATASET_VERSION_PATTERN = "v(?<major>[0-9]+).(?<minor>[0-9]+)";
    private static final String EXTENSION_PATTERN = "(?<extension>.zip|.xml)";
    private static final Pattern PATTERN = Pattern.compile(DOI_PATTERN + SCHEMA_PATTERN + DATASET_VERSION_PATTERN + EXTENSION_PATTERN);
    private final ObjectMapper objectMapper;
    private final FileService fileService;

    public TransferItemMetadataReaderImpl(ObjectMapper objectMapper, FileService fileService) {
        this.objectMapper = objectMapper;
        this.fileService = fileService;
    }

    @Override
    public FilenameAttributes getFilenameAttributes(Path path) throws InvalidTransferItemException {
        var filename = path.getFileName();
        var matcher = PATTERN.matcher(filename.toString());
        var result = new FilenameAttributes();
        result.setDveFilePath(path.toString());

        if (matcher.matches()) {
            if (matcher.group("doi") != null) {
                String datasetPid = matcher.group("doi").substring(4).toUpperCase().replaceFirst("-", ".").replaceAll("-", "/");
                result.setDatasetPid(datasetPid);
            }
            if (matcher.group("major") != null) {
                result.setVersionMajor(Integer.parseInt(matcher.group("major")));
            }
            if (matcher.group("minor") != null) {
                result.setVersionMinor(Integer.parseInt(matcher.group("minor")));
            }
        }
        else {
            throw new InvalidTransferItemException(String.format("filename %s does not match expected pattern", filename));
        }

        return result;
    }

    @Override
    public FilesystemAttributes getFilesystemAttributes(Path path) throws InvalidTransferItemException {
        var result = new FilesystemAttributes();

        try {
            var creationTime = fileService.getFilesystemAttribute(path, "creationTime");

            if (creationTime != null) {
                result.setCreationTime(LocalDateTime.ofInstant(((FileTime)creationTime).toInstant(), ZoneId.systemDefault()));
                result.setBagChecksum(fileService.calculateChecksum(path));
                result.setBagSize(fileService.getFileSize(path));
            }
        }
        catch (IOException e) {
            throw new InvalidTransferItemException(String.format("unable to read filesystem attributes for file %s", path.toString()), e);
        }

        return result;
    }

    @Override
    public FileContentAttributes getFileContentAttributes(Path path) throws InvalidTransferItemException {
        var result = new FileContentAttributes();

        // TODO move zip stuff to separate class
        try {
            ZipFile datasetVersionExport = new ZipFile(path.toFile());
            String metadataFilePath = Objects.requireNonNull(datasetVersionExport.stream().filter(zipEntry -> zipEntry.getName().endsWith(".jsonld")).findAny().orElse(null),
                "metadataFilePath can't be null: " + path).getName();
            String pidMappingFilePath = Objects.requireNonNull(datasetVersionExport.stream().filter(zipEntry -> zipEntry.getName().contains("pid-mapping.txt")).findAny().orElse(null),
                "pidMappingFilePath can't be null: " + path).getName();
            byte[] pidMapping = Objects.requireNonNull(IOUtils.toByteArray(datasetVersionExport.getInputStream(datasetVersionExport.getEntry(pidMappingFilePath))),
                "pidMapping can't be null: " + path);
            byte[] oaiOre = Objects.requireNonNull(IOUtils.toByteArray(datasetVersionExport.getInputStream(datasetVersionExport.getEntry(metadataFilePath))),
                "oaiOre can't be null: " + path);

            JsonNode jsonNode = Objects.requireNonNull(objectMapper.readTree(oaiOre), "jsonld metadata can't be null: " + path);

            String nbn = Objects.requireNonNull(jsonNode.get("ore:describes").get("dansDataVaultMetadata:NBN").asText(), "NBN can't be null: " + path);
            String dvPidVersion = Objects.requireNonNull(jsonNode.get("ore:describes").get("dansDataVaultMetadata:DV PID Version").asText(), "DV PID Version can't be null: " + path);
            String bagId = Objects.requireNonNull(jsonNode.get("ore:describes").get("dansDataVaultMetadata:Bag ID").asText(), "Bag ID can't be null: " + path);

            //TODO Other ID, Other ID Version and SWORD token missing
            result.setPidMapping(pidMapping);
            result.setOaiOre(oaiOre);
            result.setNbn(nbn);
            result.setDatasetVersion(dvPidVersion);
            result.setBagId(bagId);
        }
        catch (IOException e) {
            throw new InvalidTransferItemException(String.format("unable to read zip file contents for file %s", path), e);
        }

        return result;
    }

    @Override
    public Optional<Path> getAssociatedXmlFile(Path path) {
        Matcher matcher = PATTERN.matcher(path.getFileName().toString());
        String xml = matcher.matches() ? matcher.group("doi") + "-datacite.v" + matcher.group("major") + "." + matcher.group("minor") + ".xml" : null;

        if (xml != null) {
            return Optional.of(path.getParent().resolve(Path.of(xml)));
        }

        return Optional.empty();
    }
}
