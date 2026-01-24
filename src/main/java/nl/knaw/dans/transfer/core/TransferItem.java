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

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.bagit.exceptions.InvalidBagitFileFormatException;
import nl.knaw.dans.bagit.exceptions.MaliciousPathException;
import nl.knaw.dans.bagit.exceptions.UnparsableVersionException;
import nl.knaw.dans.bagit.exceptions.UnsupportedAlgorithmException;
import nl.knaw.dans.bagit.reader.BagReader;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.ProviderNotFoundException;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.OffsetDateTime;
import java.util.Optional;

/**
 * A Dataset Version Export (DVE) and auxiliary files. The DVE is the only mandatory file. The other files are searched next to the DVE or constructed from the DVE. This class is intended to provide
 * lightweight access to the DVE and its properties. It is not intended to be a full-fledged DVE reader or writer.
 */
@Slf4j
public class TransferItem {
    private static final String ERROR_LOG_SUFFIX = "-error.log";
    private static final String METADATA_PATH = "metadata/oai-ore.jsonld";
    private static final String NBN_JSON_PATH = "$['ore:describes']['dansDataVaultMetadata:dansNbn']";
    private static final String DATAVERSE_PID_VERSION_JSON_PATH = "$['ore:describes']['dansDataVaultMetadata:dansDataversePidVersion']";
    private static final String HAS_ORGANIZATIONAL_IDENTIFIER_VERSION = "Has-Organizational-Identifier-Version";
    
    private Path dve;
    private FileService fileService;

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

    public void moveToDir(Path dir) throws IOException {
        moveToDir(dir, (String) null);
    }

    public void moveToDir(Path dir, Exception e) throws IOException {
        moveToDir(dir, writeStackTrace(e));
    }

    /**
     * Moves the DVE to a new directory. If the file already exists with the same MD5 hash, it will be deleted.
     *
     * @param dir   the directory to move the DVE to
     * @param error the exception to log in the error log file
     * @throws IOException if an I/O error occurs
     */
    public void moveToDir(Path dir, String error) throws IOException {
        Path newLocation = dve;
        if (error == null && new DveFileName(dve).getCreationTime() == null) {  // If there is an error, keep the original name; the error might have to do with naming
            newLocation = new DveFileName(dve).withCreationTime(getCreationTimeFromFilesystem(dve)).getPath();
        }
        newLocation = findFreeName(dir, newLocation);
        if (fileService.exists(newLocation)) {
            // Should not be possible, as we have just looked for a free name
            throw new IllegalStateException("File already exists: " + newLocation);
        }
        else {
            dve = fileService.move(dve, newLocation);
        }
        if (error != null) {
            var errorLogFile = newLocation.resolveSibling(newLocation.getFileName() + ERROR_LOG_SUFFIX);
            fileService.writeString(errorLogFile, error);
        }
    }

    private OffsetDateTime getCreationTimeFromFilesystem(Path file) {
        try {
            var attrs = fileService.readAttributes(file, BasicFileAttributes.class);
            return attrs.creationTime().toInstant().atOffset(OffsetDateTime.now().getOffset());
        }
        catch (IOException ex) {
            throw new IllegalStateException("Unable to read file attributes for: " + file, ex);
        }
    }

    private static String writeStackTrace(Exception e) {
        var sw = new StringWriter();
        var pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    private Path findFreeName(Path targetDir, Path dve) {
        var dveFileName = new DveFileName(targetDir.resolve(dve.getFileName()));
        int sequenceNumber = 1;
        while (fileService.exists(dveFileName.getPath())) {
            dveFileName = dveFileName.withIndex(sequenceNumber++);
        }
        return dveFileName.getPath();
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

        try (var zipFs = fileService.newFileSystem(dve)) {
            var rootDir = zipFs.getRootDirectories().iterator().next();
            try (var topLevelDirStream = fileService.list(rootDir)) {
                var topLevelDir = topLevelDirStream.filter(fileService::isDirectory)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No top-level directory found in DVE"));

                var bag = new BagReader().read(topLevelDir);
                /*
                 * OCFL only allows one user object for the version info, so we select the first.
                 */
                var contactNames = bag.getMetadata().get("Contact-Name");
                cachedContactName = (contactNames != null && !contactNames.isEmpty()) ? contactNames.get(0) : null;
                var contactEmails = bag.getMetadata().get("Contact-Email");
                cachedContactEmail = (contactEmails != null && !contactEmails.isEmpty()) ? contactEmails.get(0) : null;
            }
            catch (MaliciousPathException e) {
                throw new RuntimeException(e);
            }
            catch (UnparsableVersionException | UnsupportedAlgorithmException | InvalidBagitFileFormatException e) {
                throw new RuntimeException("Unable to read bag info from DVE: " + dve, e);
            }
        }
        catch (IOException e) {
            throw new RuntimeException("The file system provider is not found. Probably not a ZIP file: " + dve, e);
        }
    }

    public String getNbn() throws IOException {
        readNbn();
        return cachedNbn;
    }

    private void readNbn() throws IOException {
        if (cachedNbn != null) {
            return;
        }

        try {
            try (var zipFs = fileService.newFileSystem(dve)) {
                var rootDir = zipFs.getRootDirectories().iterator().next();
                try (var topLevelDirStream = fileService.list(rootDir)) {
                    var topLevelDir = topLevelDirStream.filter(fileService::isDirectory)
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("No top-level directory found in DVE"));

                    var metadataPath = topLevelDir.resolve(METADATA_PATH);
                    if (!fileService.exists(metadataPath)) {
                        throw new IllegalStateException("No metadata file found in DVE");
                    }

                    try (var is = fileService.newInputStream(metadataPath)) {
                        cachedNbn = JsonPath.read(is, NBN_JSON_PATH);
                    }
                    catch (PathNotFoundException e) {
                        throw new IllegalStateException("No NBN found in DVE", e);
                    }
                    catch (Exception e) {
                        throw new IllegalStateException("Unable to read NBN from metadata file", e);
                    }
                }
            }
        }
        catch (ProviderNotFoundException e) {
            throw new RuntimeException("The file system provider is not found. Probably not a ZIP file: " + dve, e);
        }
    }

    public Optional<String> getDataversePidVersion() throws IOException {
        readDataversePidVersion();
        return Optional.ofNullable(cachedDataversePidVersion);
    }

    private void readDataversePidVersion() throws IOException {
        if (cachedDataversePidVersion != null) {
            return;
        }

        try {
            try (var zipFs = fileService.newFileSystem(dve)) {
                var rootDir = zipFs.getRootDirectories().iterator().next();
                try (var topLevelDirStream = fileService.list(rootDir)) {
                    var topLevelDir = topLevelDirStream.filter(fileService::isDirectory)
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("No top-level directory found in DVE"));

                    var metadataPath = topLevelDir.resolve(METADATA_PATH);
                    if (!fileService.exists(metadataPath)) {
                        log.warn("No metadata file found in DVE at {}", metadataPath);
                        return;
                    }

                    try (var is = fileService.newInputStream(metadataPath)) {
                        cachedDataversePidVersion = JsonPath.read(is, DATAVERSE_PID_VERSION_JSON_PATH);
                    }
                    catch (PathNotFoundException e) {
                        log.warn("No Dataverse PID version found in DVE at {}", metadataPath);
                    }
                    catch (Exception e) {
                        throw new IllegalStateException("Unable to read Dataverse PID version from metadata file", e);
                    }
                }
            }
        }
        catch (IOException | ProviderNotFoundException e) {
            throw new RuntimeException("The file system provider is not found. Probably not a ZIP file: " + dve, e);
        }
    }

    public Optional<String> getHasOrganizationalIdentifierVersion() throws IOException {
        readHasOrganizationalIdentifierVersion();
        return Optional.ofNullable(cachedHasOrganizationalIdentifierVersion);
    }

    private void readHasOrganizationalIdentifierVersion() throws IOException {
        if (cachedHasOrganizationalIdentifierVersion != null) {
            return;
        }

        try {
            try (var zipFs = fileService.newFileSystem(dve)) {
                var rootDir = zipFs.getRootDirectories().iterator().next();
                try (var topLevelDirStream = fileService.list(rootDir)) {
                    var topLevelDir = topLevelDirStream.filter(fileService::isDirectory)
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("No top-level directory found in DVE"));

                    var bag = new BagReader().read(topLevelDir);
                    var values = bag.getMetadata().get(HAS_ORGANIZATIONAL_IDENTIFIER_VERSION);
                    if (values != null && !values.isEmpty()) {
                        cachedHasOrganizationalIdentifierVersion = values.get(0);
                    }
                }
                catch (MaliciousPathException e) {
                    throw new RuntimeException(e);
                }
                catch (UnparsableVersionException | UnsupportedAlgorithmException | InvalidBagitFileFormatException e) {
                    throw new RuntimeException("Unable to read bag info from DVE: " + dve, e);
                }
            }
        }
        catch (ProviderNotFoundException e) {
            throw new RuntimeException("The file system provider is not found. Probably not a ZIP file: " + dve, e);
        }
    }
}
