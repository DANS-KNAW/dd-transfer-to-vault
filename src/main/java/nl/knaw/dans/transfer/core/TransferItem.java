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
package nl.knaw.dans.transfer.core;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.bagit.exceptions.InvalidBagitFileFormatException;
import nl.knaw.dans.bagit.exceptions.MaliciousPathException;
import nl.knaw.dans.bagit.exceptions.UnparsableVersionException;
import nl.knaw.dans.bagit.exceptions.UnsupportedAlgorithmException;
import nl.knaw.dans.bagit.reader.BagReader;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.ProviderNotFoundException;
import java.util.Optional;
import java.util.function.Function;

/**
 * A Dataset Version Export (DVE) and auxiliary files. The DVE is the only mandatory file. The other files are searched next to the DVE or constructed from the DVE. This class is intended to provide
 * lightweight access to the DVE and its properties. It is not intended to be a full-fledged DVE reader or writer.
 */
@Slf4j
public class TransferItem {
    private static final String METADATA_PATH = "metadata/oai-ore.jsonld";
    private static final String NBN_JSON_PATH = "$['ore:describes']['dansDataVaultMetadata:dansNbn']";
    private static final String DATAVERSE_PID_VERSION_JSON_PATH = "$['ore:describes']['dansDataVaultMetadata:dansDataversePidVersion']";
    private static final String DATAVERSE_WORK_STATUS_JSON_PATH = "$['ore:describes']['schema:creativeWorkStatus']";
    private static final String HAS_ORGANIZATIONAL_IDENTIFIER_VERSION = "Has-Organizational-Identifier-Version";

    private Path dve;
    private final FileService fileService;

    /**
     * The NBN is included in the DVE metadata and thus considered an internal property. It is cached after the first read.
     */
    private String cachedNbn;

    private String cachedContactName;

    private String cachedContactEmail;

    private String cachedDataversePidVersion;

    private String cachedHasOrganizationalIdentifierVersion;

    public TransferItem(Path dve, FileService fileService) {
        this.dve = dve;
        this.fileService = fileService;
    }

    /**
     * Gets the OCFL object version from the DVE filename. If not present, returns 0.
     *
     * @return the OCFL object version
     */
    public int getOcflObjectVersion() {
        var fileName = new DveFileName(dve);
        return fileName.getOcflObjectVersion() == null ? 0 : fileName.getOcflObjectVersion();
    }

    public void moveToTargetDirIn(Path outboxProcessed) throws IOException {
        moveToTargetDirIn(outboxProcessed, false);
    }

    public void moveToTargetDirIn(Path outboxProcessed, boolean addTimestampToFileName) throws IOException {
        fileService.moveToTargetFor(dve, outboxProcessed, getNbn(), addTimestampToFileName);
    }

    public void moveToDir(Path dir) throws IOException {
        var freeName = fileService.findFreeName(dir, dve);
        fileService.move(dve, dir.resolve(freeName));
    }

    public void moveToErrorBox(Path dir, Exception e) throws IOException {
        fileService.moveAndWriteErrorLog(dve, dir, e);
    }

    public void setOcflObjectVersion(int ocflObjectVersion) {
        var newDve = new DveFileName(dve).withOcflObjectVersion(ocflObjectVersion).getPath();
        try {
            dve = fileService.move(dve, newDve);
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to rename DVE file to include OCFL object version", e);
        }
    }

    // Helper: open zip filesystem and provide the discovered top-level dataset directory
    private <T> T withTopLevelDir(Function<Path, T> action) throws IOException {
        try (var zipFs = fileService.newFileSystem(dve)) {
            var rootDir = zipFs.getRootDirectories().iterator().next();
            try (var topLevelDirStream = fileService.list(rootDir)) {
                var topLevelDir = topLevelDirStream
                    .filter(fileService::isDirectory)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No top-level directory found in DVE"));
                return action.apply(topLevelDir);
            }
        }
        catch (ProviderNotFoundException e) {
            throw new RuntimeException("The file system provider is not found. Probably not a ZIP file: " + dve, e);
        }
    }

    // Helper: resolve metadata file path and ensure it exists
    private Path resolveMetadataPath(Path topLevelDir) {
        var metadataPath = topLevelDir.resolve(METADATA_PATH);
        if (!fileService.exists(metadataPath)) {
            throw new IllegalStateException("No metadata file found in DVE at " + metadataPath);
        }
        return metadataPath;
    }

    // Helper: read metadata json into a JsonPath DocumentContext once, to avoid reopening streams
    private DocumentContext readMetadata(Path metadataPath) {
        try (var is = fileService.newInputStream(metadataPath)) {
            return JsonPath.parse(is);
        }
        catch (IOException e) {
            throw new IllegalStateException("Unable to read metadata file: " + metadataPath, e);
        }
    }

    // Helper: read a single JSON path value as String from metadata, returns Optional.empty on missing path
    private Optional<String> readMetadataValue(DocumentContext json, String jsonPath) {
        try {
            var value = json.read(jsonPath, String.class);
            return Optional.ofNullable(value);
        }
        catch (PathNotFoundException e) {
            return Optional.empty();
        }
        catch (Exception e) {
            throw new IllegalStateException("Unable to read value from metadata json path: " + jsonPath, e);
        }
    }

    // Helper: read first value of a BagInfo key
    private Optional<String> readBagInfoFirstValue(Path topLevelDir, String key) {
        try {
            var bag = new BagReader().read(topLevelDir);
            var values = bag.getMetadata().get(key);
            if (values != null && !values.isEmpty()) {
                return Optional.ofNullable(values.get(0));
            }
            return Optional.empty();
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to read bag from DVE: " + dve, e);
        }
        catch (MaliciousPathException e) {
            throw new RuntimeException(e);
        }
        catch (UnparsableVersionException | UnsupportedAlgorithmException | InvalidBagitFileFormatException e) {
            throw new RuntimeException("Unable to read bag info from DVE: " + dve, e);
        }
    }

    /**
     * Returns the reason for deaccessioning the dataset version if the dataset version is deaccessioned. If it is not deaccessioned, it returns an empty Optional.
     *
     * @return the reason for deaccessioning, or empty if not deaccessioned
     */
    public Optional<String> getDeaccessionedReason() {
        try {
            return withTopLevelDir(topLevelDir -> {
                Path metadataPath;
                try {
                    metadataPath = resolveMetadataPath(topLevelDir);
                }
                catch (IllegalStateException e) {
                    log.warn(e.getMessage());
                    return Optional.empty();
                }
                var json = readMetadata(metadataPath);

                // Check if creativeWorkStatus exists
                try {
                    Object statusObj = json.read(DATAVERSE_WORK_STATUS_JSON_PATH);
                    if (statusObj == null) {
                        return Optional.empty();
                    }
                }
                catch (PathNotFoundException e) {
                    log.warn("No creativeWorkStatus found in DVE at {}", metadataPath);
                    return Optional.empty();
                }
                catch (Exception e) {
                    throw new IllegalStateException("Unable to read creativeWorkStatus from metadata file", e);
                }

                // Read name and reason
                var statusNameOpt = readMetadataValue(json, "$['ore:describes']['schema:creativeWorkStatus']['schema:name']");
                if (statusNameOpt.isEmpty()) {
                    return Optional.empty();
                }
                if ("DEACCESSIONED".equalsIgnoreCase(statusNameOpt.get())) {
                    var reasonOpt = readMetadataValue(json, "$['ore:describes']['schema:creativeWorkStatus']['dvcore:reason']");
                    return Optional.of(reasonOpt.filter(s -> !s.isBlank()).orElse("N/a"));
                }
                return Optional.empty();
            });
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getContactName() throws IOException {
        readContactDetails();
        return cachedContactName;
    }

    public String getContactEmail() throws IOException {
        readContactDetails();
        return cachedContactEmail;
    }

    private void readContactDetails() throws IOException {
        if (cachedContactName != null && cachedContactEmail != null) {
            return;
        }

        var result = withTopLevelDir(topLevelDir -> {
            var emailOpt = readBagInfoFirstValue(topLevelDir, "Contact-Email");
            var nameOpt = readBagInfoFirstValue(topLevelDir, "Contact-Name");
            return new String[]{emailOpt.orElse(null), nameOpt.orElse(null)};
        });
        cachedContactEmail = result[0];
        // Fallback to contact email when contact name is not available
        cachedContactName = (result[1] != null && !result[1].isBlank()) ? result[1] : cachedContactEmail;
    }

    public String getNbn() throws IOException {
        readNbn();
        return cachedNbn;
    }

    private void readNbn() throws IOException {
        if (cachedNbn != null) {
            return;
        }

        cachedNbn = withTopLevelDir(topLevelDir -> {
            var metadataPath = resolveMetadataPath(topLevelDir);
            var json = readMetadata(metadataPath);
            var nbnOpt = readMetadataValue(json, NBN_JSON_PATH);
            if (nbnOpt.isEmpty()) {
                throw new IllegalStateException("No NBN found in DVE");
            }
            return nbnOpt.get();
        });
    }

    public Optional<String> getDataversePidVersion() throws IOException {
        readDataversePidVersion();
        return Optional.ofNullable(cachedDataversePidVersion);
    }

    private void readDataversePidVersion() throws IOException {
        if (cachedDataversePidVersion != null) {
            return;
        }

        cachedDataversePidVersion = withTopLevelDir(topLevelDir -> {
            var metadataPath = topLevelDir.resolve(METADATA_PATH);
            if (!fileService.exists(metadataPath)) {
                log.warn("No metadata file found in DVE at {}", metadataPath);
                return null;
            }
            var json = readMetadata(metadataPath);
            return readMetadataValue(json, DATAVERSE_PID_VERSION_JSON_PATH).orElse(null);
        });
    }

    public Optional<String> getHasOrganizationalIdentifierVersion() throws IOException {
        readHasOrganizationalIdentifierVersion();
        return Optional.ofNullable(cachedHasOrganizationalIdentifierVersion);
    }

    private void readHasOrganizationalIdentifierVersion() throws IOException {
        if (cachedHasOrganizationalIdentifierVersion != null) {
            return;
        }

        var opt = withTopLevelDir(topLevelDir ->
            readBagInfoFirstValue(topLevelDir, HAS_ORGANIZATIONAL_IDENTIFIER_VERSION)
        );
        cachedHasOrganizationalIdentifierVersion = opt.orElse(null);
    }
}
