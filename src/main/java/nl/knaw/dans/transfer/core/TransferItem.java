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
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.ProviderNotFoundException;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.OffsetDateTime;

/**
 * A Dataset Version Export (DVE) and auxiliary files. The DVE is the only mandatory file. The other files are searched next to the DVE or constructed from the DVE. This class is intended to provide
 * lightweight access to the DVE and its properties. It is not intended to be a full-fledged DVE reader or writer.
 */
@Slf4j
public class TransferItem {
    private static final String ERROR_LOG_SUFFIX = "-error.log";
    private static final String METADATA_PATH = "metadata/oai-ore.jsonld";
    private static final String BAG_INFO_PATH = "bag-info.txt";
    private static final String NBN_JSON_PATH = "$.ore:describes.dansDataVaultMetadata:dansNbn";

    private Path dve;

    /**
     * The NBN is included in the DVE metadata and thus considered an internal property. It is cached after the first read.
     */
    private String cachedNbn;

    private String cachedContactName;

    private String cachedContactEmail;

    public TransferItem(Path dve) {
        this.dve = dve;
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

    /**
     * Gets the creation time from the DVE filename. If not present, null is returned.
     *
     * @return the creation timereads it from the file system attributes.
     */
    public OffsetDateTime getCreationTime() {
        var fileName = new DveFileName(dve);
        return fileName.getCreationDateTime();
    }

    /**
     * Moves the DVE to a new directory. If the file already exists with the same MD5 hash, it will be deleted.
     *
     * @param dir the directory to move the DVE to
     * @param e   the exception to log in the error log file
     * @throws IOException if an I/O error occurs
     */
    public void moveToDir(Path dir, Exception e) throws IOException {
        var newLocation = findFreeName(dir, e == null ? addCreationTimeIfNeeded(dve) : dve); // Don't bother adding creation time if we are going to log an error anyway
        var tempNewLocation = newLocation.resolveSibling(newLocation.getFileName() + ".tmp");
        if (Files.exists(newLocation)) {
            throw new IllegalStateException("File already exists: " + newLocation);
        }
        else {
            Files.move(dve, tempNewLocation); // Make sure the file is not detected before the move is complete
            // Hard sync file system
            try (var channel = FileChannel.open(tempNewLocation)) {
                channel.force(true);
            }
            Files.move(tempNewLocation, newLocation);
            dve = newLocation;
        }
        if (e != null) {
            var errorLogFile = newLocation.resolveSibling(newLocation.getFileName() + ERROR_LOG_SUFFIX);
            writeStackTrace(errorLogFile, e);
        }
    }

    /*
     * This should only happen the first time the DVE is moved. This first time it will be move over a partition boundary, which means the creation time will not be
     * preserved in the file system.
     */
    private Path addCreationTimeIfNeeded(Path dve) {
        var fileName = new DveFileName(dve);
        if (fileName.getCreationDateTime() != null) {
            return dve;
        }
        var creationTimeMillis = getCreationTimeFromFilesystem(dve);
        var dotIndex = dve.getFileName().toString().lastIndexOf('.');
        var baseName = (dotIndex == -1) ? dve.getFileName().toString() : dve.getFileName().toString().substring(0, dotIndex);
        var extension = (dotIndex == -1) ? "" : dve.getFileName().toString().substring(dotIndex);
        var newFileName = String.format("%s_%d%s", baseName, creationTimeMillis, extension);
        return dve.resolveSibling(newFileName);
    }

    private long getCreationTimeFromFilesystem(Path file) {
        try {
            var attrs = Files.readAttributes(file, BasicFileAttributes.class);
            return attrs.creationTime().toMillis();
        }
        catch (IOException ex) {
            throw new IllegalStateException("Unable to read file attributes for: " + file, ex);
        }
    }

    private static void writeStackTrace(Path errorLog, Exception e) throws IOException {
        try (var writer = Files.newBufferedWriter(errorLog)) {
            e.printStackTrace(new java.io.PrintWriter(writer));
        }
    }

    private Path findFreeName(Path targetDir, Path dve) throws IOException {
        var fileName = dve.getFileName().toString();
        var dotIndex = fileName.lastIndexOf('.');
        var baseName = (dotIndex == -1) ? fileName : fileName.substring(0, dotIndex);
        var extension = (dotIndex == -1) ? "" : fileName.substring(dotIndex);
        var sequenceNumber = 0;
        Path newPath;
        do {
            if (sequenceNumber == 0) {
                newPath = targetDir.resolve(baseName + extension);
            }
            else {
                newPath = targetDir.resolve(baseName + "-" + sequenceNumber + extension);
            }
            sequenceNumber++;
        }
        while (Files.exists(newPath));
        return newPath;
    }

    public void moveToDir(Path dir) throws IOException {
        moveToDir(dir, null);
    }

    public void setOcflObjectVersion(int ocflObjectVersion) {
        int currentVersion = getOcflObjectVersion();
        if (currentVersion != 0 && currentVersion != ocflObjectVersion) {
            throw new IllegalStateException("OCFL object version is already set to " + currentVersion + ", cannot change to " + ocflObjectVersion);
        }
        try {
            var newDve = dve.resolveSibling(dve.getFileName() + "_v" + ocflObjectVersion);
            Files.move(dve, newDve);
            dve = newDve;
            try (var channel = FileChannel.open(dve)) {
                channel.force(true);
            }
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

        try (FileSystem zipFs = FileSystems.newFileSystem(dve, (ClassLoader) null)) {
            var rootDir = zipFs.getRootDirectories().iterator().next();
            try (var topLevelDirStream = Files.list(rootDir)) {
                var topLevelDir = topLevelDirStream.filter(Files::isDirectory)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No top-level directory found in DVE"));

                var bag = new BagReader().read(topLevelDir);
                cachedContactName = String.join(";", bag.getMetadata().get("Contact-Name"));
                cachedContactEmail = String.join(";", bag.getMetadata().get("Contact-Email"));
            }
            catch (MaliciousPathException e) {
                throw new RuntimeException(e);
            }
            catch (UnparsableVersionException | UnsupportedAlgorithmException | InvalidBagitFileFormatException e) {
                throw new RuntimeException("Unable to read bag info from DVE: " + dve, e);
            }
            catch (ProviderNotFoundException e) {
                throw new RuntimeException("The file system provider is not found. Probably not a ZIP file: " + dve, e);
            }
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

        try (FileSystem zipFs = FileSystems.newFileSystem(dve, (ClassLoader) null)) {
            var rootDir = zipFs.getRootDirectories().iterator().next();
            try (var topLevelDirStream = Files.list(rootDir)) {
                var topLevelDir = topLevelDirStream.filter(Files::isDirectory)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No top-level directory found in DVE"));

                var metadataPath = topLevelDir.resolve(METADATA_PATH);
                if (!Files.exists(metadataPath)) {
                    throw new IllegalStateException("No metadata file found in DVE");
                }

                try (var is = Files.newInputStream(metadataPath)) {
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
        catch (ProviderNotFoundException e) {
            throw new RuntimeException("The file system provider is not found. Probably not a ZIP file: " + dve, e);
        }
    }
}
