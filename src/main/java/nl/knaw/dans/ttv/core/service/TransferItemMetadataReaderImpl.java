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
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public class TransferItemMetadataReaderImpl implements TransferItemMetadataReader {
    private static final Pattern DATAVERSE_PATTERN = Pattern.compile(
        "(?<doi>doi-10-[0-9]{4,}-[A-Za-z0-9]{2,}-[A-Za-z0-9]{6})-?" +
            "(?<schema>datacite)?.?" +
            "v(?<major>[0-9]+).(?<minor>[0-9]+)"
    );
    private static final List<Pattern> VALID_PATTERNS = List.of(
        DATAVERSE_PATTERN,
        // the dataverse output
        // the vault ingest flow output (uuid + version)
        Pattern.compile("^vaas-[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{12}-v\\d+$")
    );
    private static final Pattern TTV_SUFFIX = Pattern.compile("(?<identifier>.*)(?<suffix>-ttv\\d+)");
    private static final Set<String> ALLOWED_EXTENSIONS = Set.of("zip");
    private final ObjectMapper objectMapper;
    private final FileService fileService;

    public TransferItemMetadataReaderImpl(ObjectMapper objectMapper, FileService fileService) {
        this.objectMapper = objectMapper;
        this.fileService = fileService;
    }

    @Override
    public FilenameAttributes getFilenameAttributes(Path path) throws InvalidTransferItemException {
        var filename = normalizeFilename(path.getFileName().toString());
        var extension = FilenameUtils.getExtension(path.getFileName().toString());
        var filenameIsExpected = filenameMatchesPatterns(filename, extension);
        var internalId = getInternalId(path.getFileName().toString());

        if (!filenameIsExpected) {
            throw new InvalidTransferItemException(String.format("filename %s does not match expected pattern(s)", path.getFileName()));
        }

        return FilenameAttributes.builder()
            .dveFilePath(path.toString())
            .identifier(filename)
            .internalId(internalId)
            .build();
    }

    private String normalizeFilename(String filename) {
        filename = FilenameUtils.removeExtension(filename);

        var suffixMatch = TTV_SUFFIX.matcher(filename);

        if (suffixMatch.matches()) {
            filename = suffixMatch.group("identifier");
        }

        return filename;
    }

    private Long getInternalId(String filename) {
        filename = FilenameUtils.removeExtension(filename);

        var suffixMatch = TTV_SUFFIX.matcher(filename);

        if (suffixMatch.matches()) {
            var number = suffixMatch.group("suffix")
                .replace("-ttv", "");

            return Long.parseLong(number);
        }

        return null;
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

    private boolean filenameMatchesPatterns(String filename, String extension) {
        if (!ALLOWED_EXTENSIONS.contains(extension)) {
            return false;
        }

        for (var pattern : VALID_PATTERNS) {
            if (pattern.matcher(filename).matches()) {
                return true;
            }
        }

        return false;
    }
}
