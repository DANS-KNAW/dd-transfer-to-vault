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
import nl.knaw.dans.ttv.core.domain.FileContentAttributes;
import nl.knaw.dans.ttv.core.domain.FilenameAttributes;
import nl.knaw.dans.ttv.core.domain.FilesystemAttributes;
import nl.knaw.dans.ttv.core.domain.Version;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

        if (matcher.matches()) {

            var datasetPid = Optional.ofNullable(matcher.group("doi"))
                .map(doi -> doi.substring(4).toUpperCase().replaceFirst("-", ".").replaceAll("-", "/"))
                .orElseThrow(() -> new InvalidTransferItemException(String.format("filename %s does not contain a DOI", filename)));

            var major = Optional.ofNullable(matcher.group("major"))
                .map(Integer::parseInt).orElse(0);

            var minor = Optional.ofNullable(matcher.group("minor"))
                .map(Integer::parseInt).orElse(0);

            return FilenameAttributes.builder()
                .dveFilePath(path.toString())
                .datasetPid(datasetPid)
                .version(Version.of(major, minor))
                .build();
        }
        else {
            throw new InvalidTransferItemException(String.format("filename %s does not match expected pattern", filename));
        }
    }

    @Override
    public FilesystemAttributes getFilesystemAttributes(Path path) throws InvalidTransferItemException {
        var result = new FilesystemAttributes();

        try {
            var creationTime = fileService.getFilesystemAttribute(path, "creationTime");

            if (creationTime != null) {
                result.setCreationTime(LocalDateTime.ofInstant(((FileTime) creationTime).toInstant(), ZoneId.systemDefault()));
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

        try {
            var datasetVersionExport = fileService.openZipFile(path);

            var metadataContent = fileService.openFileFromZip(datasetVersionExport, Path.of("metadata/oai-ore.jsonld"));
            var pidMappingContent = fileService.openFileFromZip(datasetVersionExport, Path.of("metadata/pid-mapping.txt"));

            var oaiOre = IOUtils.toString(metadataContent, StandardCharsets.UTF_8);
            var pidMapping = IOUtils.toString(pidMappingContent, StandardCharsets.UTF_8);

            var jsonNode = Objects.requireNonNull(objectMapper.readTree(oaiOre), "jsonld metadata can't be null: " + path);
            var describesNode = Objects.requireNonNull(jsonNode.get("ore:describes"), "ore:describes node can't be null");

            var nbn = getStringFromNode(describesNode, "dansDataVaultMetadata:NBN");
            var dvPidVersion = getStringFromNode(describesNode, "dansDataVaultMetadata:DV PID Version");
            var bagId = getStringFromNode(describesNode, "dansDataVaultMetadata:Bag ID");
            var otherId = getOptionalStringFromNode(describesNode, "dansDataVaultMetadata:Other ID");
            var otherIdVersion = getOptionalStringFromNode(describesNode, "dansDataVaultMetadata:Other ID Version");
            var swordClient = getOptionalStringFromNode(describesNode, "dansDataVaultMetadata:SWORD Client");
            var swordToken = getOptionalStringFromNode(describesNode, "dansDataVaultMetadata:SWORD Token");

            return FileContentAttributes.builder()
                .bagChecksum(fileService.calculateChecksum(path))
                .pidMapping(pidMapping)
                .oaiOre(oaiOre)
                .nbn(nbn)
                .datasetVersion(dvPidVersion)
                .bagId(bagId)
                .otherId(otherId)
                .otherIdVersion(otherIdVersion)
                .swordToken(swordToken)
                .swordClient(swordClient)
                .build();
        }
        catch (IOException e) {
            throw new InvalidTransferItemException(String.format("unable to read zip file contents for file '%s'", path), e);
        }
        catch (NullPointerException e) {
            throw new InvalidTransferItemException(String.format("unable to extract metadata from file '%s'", path), e);
        }
    }

    private String getStringFromNode(JsonNode node, String path) {
        return Objects.requireNonNull(node.get(path), String.format("path '%s' not found in JSON node", path)).asText();
    }

    private String getOptionalStringFromNode(JsonNode node, String path) {
        return Optional.ofNullable(node.get(path))
            .map(JsonNode::asText)
            .orElse(null);
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
