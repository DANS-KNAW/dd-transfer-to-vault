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
import java.util.UUID;

public class TarTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TarTask.class);

    private final TransferItemService transferItemService;
    private final Path inboxPath;
    private final UUID uuid;
    private final TarCommandRunner tarCommandRunner;
    private final ArchiveMetadataService archiveMetadataService;

    public TarTask(TransferItemService transferItemService, UUID uuid, Path inboxPath, TarCommandRunner tarCommandRunner, ArchiveMetadataService archiveMetadataService) {
        this.transferItemService = transferItemService;
        this.inboxPath = inboxPath;
        this.uuid = uuid;
        this.tarCommandRunner = tarCommandRunner;
        this.archiveMetadataService = archiveMetadataService;
    }

    @Override
    public void run() {
        try {
            createTarArchive();
            // TODO catch exceptions where one of these things went wrong:
            // - tar wasnt uploaded due to IOException (or code is not 0)
            // - verify step failed
            // if so, remove remote tar and reset status to TARRING, retry it again
        }
        catch (IOException | InterruptedException e) {
            log.error("error while creating TAR archive", e);
        }
    }

    private void createTarArchive() throws IOException, InterruptedException {
        // run dmftar and upload results
        log.info("Tarring directory '{}' with UUID {}", this.inboxPath, uuid);
        tarDirectory(uuid, this.inboxPath);

        log.info("Verifying remote TAR with UUID {}", this.uuid);
        verifyTarArchive(uuid);

        log.info("Extracting metadata from remote TAR with UUID {}", this.uuid);
        var metadata = extractTarMetadata(uuid);

        log.info("Extracted metadata for remote TAR with uuid {}: {}", this.uuid, metadata);
        transferItemService.updateTarToCreated(uuid.toString(), metadata);
    }

    private void tarDirectory(UUID packageName, Path sourceDirectory) throws IOException, InterruptedException {
        var targetPackage = getTarArchivePath(packageName);
        var result = tarCommandRunner.tarDirectory(sourceDirectory, targetPackage);

        // it failed
        if (result.getStatusCode() != 0) {
            throw new IOException(String.format("unable to tar folder '%s' to location '%s", sourceDirectory, targetPackage));
        }
    }

    private void verifyTarArchive(UUID packageName) throws IOException, InterruptedException {
        var targetPackage = getTarArchivePath(packageName);
        var result = tarCommandRunner.verifyPackage(targetPackage);

        if (result.getStatusCode() != 0) {
            throw new IOException(String.format("tar folder '%s' cannot be verified: %s", targetPackage, result.getStdout()));
        }
    }

    private ArchiveMetadata extractTarMetadata(UUID packageName) throws IOException, InterruptedException {
        return archiveMetadataService.getArchiveMetadata(packageName.toString());
    }

    private String getTarArchivePath(UUID uuid) {
        return uuid.toString() + ".dmftar";
    }

}

