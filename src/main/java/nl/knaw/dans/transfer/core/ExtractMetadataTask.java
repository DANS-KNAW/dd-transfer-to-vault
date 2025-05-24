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
import nl.knaw.dans.transfer.client.VaultCatalogClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;

@Slf4j
@AllArgsConstructor
public class ExtractMetadataTask implements Runnable {
    private final String ocflStorageRoot;
    private final Path targetNbnDir;
    private final Path outboxProcessed;
    private final Path outboxFailed;
    private final Path outboxRejected;
    private final DveMetadataReader dveMetadataReader;
    private final VaultCatalogClient vaultCatalogClient;

    @Override
    public void run() {
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

        try {
            var dves = getDves();
            while (Files.exists(targetNbnDir)) {
                for (var dve : dves) {
                    TransferItem transferItem = null;
                    try {
                        transferItem = new TransferItem(dve);

                        // TODO: validate DVE (call dd-validate-bagpack)

                        transferItem.setOcflObjectVersion(
                            vaultCatalogClient.registerOcflObjectVersion(ocflStorageRoot, dveMetadataReader.readDveMetadata(dve), transferItem.getOcflObjectVersion()));

                        // TODO: schedule NBN registration

                        transferItem.moveToDir(outboxProcessed);
                    }
                    catch (Exception e) {
                        log.error("Error processing DVE", e);
                        if (transferItem != null) {
                            transferItem.moveToDir(outboxFailed, e);
                        }
                        else {
                            log.error("TransferItem is null, unable to move DVE to failed outbox");
                        }
                        try {
                            blockTarget();
                        }
                        catch (IOException ioe) {
                            log.error("Unable to block target directory", ioe);
                        }
                    }
                }
                // Wait 100ms before checking for new files
                try {
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
        }
        catch (Exception e) {
            log.error("Error processing DVE files", e);
            try {
                blockTarget();
            }
            catch (IOException ioe) {
                log.error("Unable to block target directory", ioe);
            }
        }
        finally {
            log.debug("Finished ExtractMetadataTask for {}", targetNbnDir);
        }
        /*
         * At this point, all DVE files in targetNbnDir have been processed. We are NOT sticking around to wait for more DVEs, as this would prevent our worker thread from taking on other tasks.
         * The transfer inbox will dete targetNbnDir if it is empty in the next polling cycle. If new DVEs are added for this same NBN, the inbox will create a new targetNbnDir (with a different name)
         * and create a new ExtractMetadataTask for it. It cannot have the same name, because there would be two tasks instances competing for the same targetNbnDir.
         */
    }

    private List<Path> getDves() throws IOException {
        try (var dirStream = Files.list(targetNbnDir)) {
            return dirStream.filter(Files::isRegularFile).filter(p -> p.getFileName().toString().endsWith(".zip"))
                // TODO: use creationTime in the properties file instead
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
        if (!isBlocked()) {
            Files.createFile(targetNbnDir.resolve("block"));
        }
    }
}
