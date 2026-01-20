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

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.util.healthcheck.DependenciesReadyCheck;
import nl.knaw.dans.transfer.client.ValidateBagPackClient;
import nl.knaw.dans.transfer.client.VaultCatalogClient;
import nl.knaw.dans.transfer.health.HealthChecks;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

@Slf4j
@AllArgsConstructor
public class ExtractMetadataTask implements Runnable {
    private final String ocflStorageRoot;
    private final Path targetNbnDir;
    private final Path outboxProcessed;
    private final Path outboxFailed;
    private final Path outboxRejected;
    private final Path nbnRegistrationInbox;
    private final URI vaultCatalogBaseUri;
    private final DveMetadataReader dveMetadataReader;
    private final FileService fileService;
    private final VaultCatalogClient vaultCatalogClient;
    private final ValidateBagPackClient validateBagPackClient;
    private final DependenciesReadyCheck readyCheck;

    @Override
    public void run() {
        log.debug("Started ExtractMetadataTask for {}", targetNbnDir);
        readyCheck.waitUntilReady(HealthChecks.FILESYSTEM_PERMISSIONS, HealthChecks.VALIDATE_BAG_PACK, HealthChecks.VAULT_CATALOG);
        log.debug("Readycheck complete");
        log.debug("Started ExtractMetadataTask for {}", targetNbnDir);
        try {
            if (isBlocked()) {
                log.debug("Target directory {} is blocked, skipping", targetNbnDir);
                return;
            }
        }
        catch (IOException e) {
            log.error("Unable to check if target directory is blocked. Aborting...", e);
            return;
        }

        TransferItem transferItem = null;
        try {
            var dves = getDves();
            while (Files.exists(targetNbnDir)) {
                log.debug("Found {} DVEs to process", dves.size());
                for (var dve : dves) {
                    transferItem = new TransferItem(dve);

                    log.debug("Validating DVE {} as BagPack...", dve);
                    var result = validateBagPackClient.validateBagPack(dve);
                    if (result.getIsCompliant()) {
                        log.info("DVE {} is a compliant BagPack.", dve);
                    }
                    else {
                        log.warn("DVE {} is not a compliant BagPack: {}. Moving to rejected outbox.", dve, result.getRuleViolations());
                        transferItem.moveToDir(outboxRejected, result.getRuleViolations().toString());
                        blockTarget();
                        return;
                    }

                    log.debug("Reading metadata from dve");
                    var dveMetadata = dveMetadataReader.readDveMetadata(dve);
                    if (dveMetadata.getContactName() == null || dveMetadata.getContactEmail() == null) {
                        throw new IllegalArgumentException("Missing contact information in DVE metadata");
                    }

                    log.debug("Registering OCFL Object Version in Vault Catalog");
                    transferItem.setOcflObjectVersion(
                        vaultCatalogClient.registerOcflObjectVersion(ocflStorageRoot, dveMetadata, transferItem.getOcflObjectVersion()));
                    if (transferItem.getOcflObjectVersion() == 1) {
                        log.info("First version of dataset {}. Scheduling NBN registration", transferItem.getNbn());
                        scheduleNbnRegistration(transferItem);
                    }
                    log.debug("Moving DVE to processed outbox");
                    transferItem.moveToDir(outboxProcessed);
                }

                try {
                    log.debug("Taking a short nap before resuming work");
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    log.debug("Thread interrupted, exiting");
                    Thread.currentThread().interrupt();
                    return;
                }

                // Get any new DVE files that may have been added while processing
                dves = getDves();
            }
            /*
             * At this point, all DVE files in targetNbnDir have been processed. We are NOT sticking around to wait for more DVEs, as this would prevent our
             * worker thread from taking on other tasks. The transfer inbox will dete targetNbnDir if it is empty in the next polling cycle. If new DVEs are added
             * for this same NBN, the inbox will create a new targetNbnDir (with a different name) and create a new ExtractMetadataTask for it. It cannot have
             * the same name because there would be two task instances competing for the same targetNbnDir.
             */

        }
        catch (Exception e) {
            log.error("Error processing DVE files", e);
            try {
                blockTarget();
                if (transferItem != null) {
                    transferItem.moveToDir(outboxFailed, e);
                }
                else {
                    log.warn("Failed, but no transferItem was set!");
                }
            }
            catch (IOException ioe) {
                log.error("Unable to block target directory", ioe);
            }
        }
        finally {
            log.debug("Finished ExtractMetadataTask for {}", targetNbnDir);
        }
    }

    private void scheduleNbnRegistration(TransferItem transferItem) {
        try {
            new RegistrationToken(transferItem.getNbn(), URI.create(vaultCatalogBaseUri + transferItem.getNbn()))
                .save(nbnRegistrationInbox.resolve(transferItem.getNbn() + ".properties"));
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Unable to schedule NBN registration because NBN could not be retrieved", e);
        }
    }

    private List<Path> getDves() throws IOException {
        try (var dirStream = Files.list(targetNbnDir)) {
            return dirStream.filter(Files::isRegularFile).filter(p -> p.getFileName().toString().endsWith(".zip"))
                .sorted(CreationTimeComparator.getInstance()).toList();
        }
        catch (NoSuchFileException e) {
            log.debug("Target directory {} does not exist anymore. No more DVEs to process.", targetNbnDir);
            // This can happen if the targetNbnDir is deleted just after processing the last DVE.
            return List.of();
        }
        catch (IOException e) {
            log.error("Error listing files in targetNbnDir", e);
            throw e;
        }
    }

    private boolean isBlocked() throws IOException {
        return Files.exists(targetNbnDir.resolve("block"));
    }

    private void blockTarget() throws IOException {
        var blockFile = targetNbnDir.resolve("block");
        // Create the block file
        Files.writeString(blockFile, "", StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        fileService.fsyncFile(blockFile);
        fileService.fsyncDirectory(targetNbnDir);
    }
}
