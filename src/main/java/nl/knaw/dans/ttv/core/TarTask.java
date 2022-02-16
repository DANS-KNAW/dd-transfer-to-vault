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
package nl.knaw.dans.ttv.core;

import nl.knaw.dans.ttv.core.dto.ArchiveMetadata;
import nl.knaw.dans.ttv.core.service.ArchiveMetadataService;
import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

public class TarTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TarTask.class);

    private final TransferItemService transferItemService;
    private final Path inboxPath;
    private final String uuid;
    private final TarCommandRunner tarCommandRunner;
    private final ArchiveMetadataService archiveMetadataService;
    private final int maxRetries;

    public TarTask(TransferItemService transferItemService, String uuid, Path inboxPath, TarCommandRunner tarCommandRunner, ArchiveMetadataService archiveMetadataService, int maxRetries) {
        this.transferItemService = transferItemService;
        this.inboxPath = inboxPath;
        this.uuid = uuid;
        this.tarCommandRunner = tarCommandRunner;
        this.archiveMetadataService = archiveMetadataService;
        this.maxRetries = maxRetries;
    }

    @Override
    public void run() {
        createArchive();
    }

    /**
     * Verifies the remote archive, and attempts to transfer it if it does not exist or is invalid. Only after everything is correctly transferred, it will extract metadata (checksums) from the remote
     * archive.
     *
     * Note that InterruptedException will not increase the attempt count, because interrupts
     */
    void createArchive() {
        try {
            // The first step is validating the remote archive. It might not even exist,
            // in which case we upload it.
            try {
                log.info("Verifying remote TAR with UUID {}", uuid);
                verifyArchive(uuid);
            }
            catch (IOException e) {
                log.info("Remote archive does not exist yet, or is not valid; transferring the archive again");
                transferArchive(uuid);
            }

            // if the statements above do not throw, it was archived correctly, and we can extract the metadata
            log.info("Extracting metadata from remote TAR with UUID {}", uuid);
            var metadata = extractTarMetadata(uuid);

            log.info("Extracted metadata for remote TAR with uuid {}: {}", uuid, metadata);
            transferItemService.updateTarToCreated(uuid, metadata);
        }
        catch (IOException e) {
            log.error("Unable to transfer archive, marking for retry", e);
            transferItemService.setArchiveAttemptFailed(uuid, true, maxRetries);
        }
        catch (InterruptedException e) {
            log.error("Unable to transfer archive due to InterruptedException", e);
            transferItemService.setArchiveAttemptFailed(uuid, false, maxRetries);
        }
    }

    /**
     * Transfers the archive to the remote location. First it deletes existing archives with the same name if they exist. After the transfer it verifies the contents have been correctly transferred.
     * If not, it deletes the remote archive again.
     *
     * @param uuid
     * @throws InterruptedException
     * @throws IOException
     */
    void transferArchive(String uuid) throws InterruptedException, IOException {
        try {
            log.info("Remote archive does not exist, or is not valid");
            deleteArchiveSilent(uuid);

            log.info("Tarring directory '{}' with UUID {}", this.inboxPath, uuid);
            tarDirectory(uuid, this.inboxPath);

            log.info("Extracting metadata from remote TAR with UUID {}", uuid);
            verifyArchive(uuid);
        }
        catch (IOException e) {
            log.error("Error transfering TAR to remote archive, cleaning up", e);
            deleteArchiveSilent(uuid);
            throw e;
        }
    }

    void tarDirectory(String packageName, Path sourceDirectory) throws IOException, InterruptedException {
        var targetPackage = getArchivePath(packageName);
        var result = tarCommandRunner.tarDirectory(sourceDirectory, targetPackage);

        // it failed
        if (result.getStatusCode() != 0) {
            throw new IOException(String.format("unable to tar folder '%s' to location '%s", sourceDirectory, targetPackage));
        }
    }

    void verifyArchive(String packageName) throws IOException, InterruptedException {
        var targetPackage = getArchivePath(packageName);
        var result = tarCommandRunner.verifyPackage(targetPackage);

        if (result.getStatusCode() != 0) {
            throw new IOException(String.format("tar folder '%s' cannot be verified: %s", targetPackage, result.getStdout()));
        }
    }

    ArchiveMetadata extractTarMetadata(String packageName) throws IOException, InterruptedException {
        return archiveMetadataService.getArchiveMetadata(packageName);
    }

    String getArchivePath(String uuid) {
        return uuid + ".dmftar";
    }

    void deleteArchiveSilent(String packageName) throws InterruptedException {
        try {
            deleteArchive(packageName);
        }
        catch (IOException e) {
            log.debug("Error deleting archive, but this is expected");
        }
    }

    void deleteArchive(String packageName) throws IOException, InterruptedException {
        var targetPackage = getArchivePath(packageName);
        var result = tarCommandRunner.deletePackage(targetPackage);

        if (result.getStatusCode() != 0) {
            throw new IOException(String.format("tar folder '%s' cannot be verified: %s", targetPackage, result.getStdout()));
        }
    }

    @Override
    public String toString() {
        return "TarTask{" +
            "inboxPath=" + inboxPath +
            ", uuid='" + uuid + '\'' +
            '}';
    }
}

