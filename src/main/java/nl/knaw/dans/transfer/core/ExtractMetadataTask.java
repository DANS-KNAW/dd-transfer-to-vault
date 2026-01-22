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

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.util.healthcheck.DependenciesReadyCheck;
import nl.knaw.dans.transfer.client.ValidateBagPackClient;
import nl.knaw.dans.transfer.client.VaultCatalogClient;
import nl.knaw.dans.transfer.health.HealthChecks;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

@Slf4j
public class ExtractMetadataTask extends SourceDirItemProcessor implements Runnable {
    private final String ocflStorageRoot;
    private final Path targetNbnDir;
    private final Path outboxProcessed;
    private final Path outboxFailed;
    private final Path outboxRejected;
    private final Path nbnRegistrationInbox;
    private final URI vaultCatalogBaseUri;
    private final DveMetadataReader dveMetadataReader;
    private final VaultCatalogClient vaultCatalogClient;
    private final ValidateBagPackClient validateBagPackClient;
    private final DependenciesReadyCheck readyCheck;

    private TransferItem currentTransferItem;

    public ExtractMetadataTask(Path srcDir, String ocflStorageRoot, Path outboxProcessed, Path outboxFailed, Path outboxRejected,
        Path nbnRegistrationInbox, URI vaultCatalogBaseUri, DveMetadataReader dveMetadataReader,
        FileService fileService, VaultCatalogClient vaultCatalogClient,
        ValidateBagPackClient validateBagPackClient, DependenciesReadyCheck readyCheck,
        long delayBetweenProcessingRounds) {
        super(srcDir, "DVE", new DveFileFilter().toPredicate(), CreationTimeComparator.getInstance(), fileService, delayBetweenProcessingRounds);
        this.targetNbnDir = srcDir;
        this.ocflStorageRoot = ocflStorageRoot;
        this.outboxProcessed = outboxProcessed;
        this.outboxFailed = outboxFailed;
        this.outboxRejected = outboxRejected;
        this.nbnRegistrationInbox = nbnRegistrationInbox;
        this.vaultCatalogBaseUri = vaultCatalogBaseUri;
        this.dveMetadataReader = dveMetadataReader;
        this.vaultCatalogClient = vaultCatalogClient;
        this.validateBagPackClient = validateBagPackClient;
        this.readyCheck = readyCheck;
    }

    @Override
    public void run() {
        log.debug("Started ExtractMetadataTask2 for {}", targetNbnDir);
        readyCheck.waitUntilReady(HealthChecks.FILESYSTEM_PERMISSIONS, HealthChecks.VALIDATE_BAG_PACK, HealthChecks.VAULT_CATALOG);
        log.debug("Readycheck complete");

        processUntilRemoved();
    }

    @Override
    protected void processItem(Path item) throws IOException {
        currentTransferItem = new TransferItem(item);

        log.debug("Validating DVE {} as BagPack...", item);
        var result = validateBagPackClient.validateBagPack(item);
        if (result.getIsCompliant()) {
            log.info("DVE {} is a compliant BagPack.", item);
        }
        else {
            log.warn("DVE {} is not a compliant BagPack: {}. Moving to rejected outbox.", item, result.getRuleViolations());
            currentTransferItem.moveToDir(outboxRejected, result.getRuleViolations().toString());
            throw new IllegalArgumentException(result.getRuleViolations().toString());
        }

        log.debug("Reading metadata from dve");
        var dveMetadata = dveMetadataReader.readDveMetadata(item);
        if (dveMetadata.getContactName() == null || dveMetadata.getContactEmail() == null) {
            throw new IllegalArgumentException("Missing contact information in DVE metadata");
        }

        log.debug("Registering OCFL Object Version in Vault Catalog");
        currentTransferItem.setOcflObjectVersion(
            vaultCatalogClient.registerOcflObjectVersion(ocflStorageRoot, dveMetadata, currentTransferItem.getOcflObjectVersion()));

        if (currentTransferItem.getOcflObjectVersion() == 1) {
            log.info("First version of dataset {}. Scheduling NBN registration", currentTransferItem.getNbn());
            scheduleNbnRegistration(currentTransferItem);
        }

        log.debug("Moving DVE to processed outbox");
        currentTransferItem.moveToDir(outboxProcessed);

        try {
            log.debug("Taking a short nap before resuming work");
            Thread.sleep(100);
        }
        catch (InterruptedException e) {
            log.debug("Thread interrupted, exiting");
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while processing items", e);
        }
    }

    @Override
    protected void rejectCurrentItem(IllegalArgumentException e) {
        log.warn("Invalid DVE, skipping: {}", e.getMessage());
        try {
            if (currentTransferItem != null) {
                currentTransferItem.moveToDir(outboxRejected, e);
            }
            else {
                log.warn("Invalid DVE, but no transferItem was set!");
            }
        }
        catch (IOException ioe) {
            log.error("Unable to block target directory or move transfer item to rejected outbox", ioe);
        }
    }

    @Override
    protected void failCurrentItem(Exception e) {
        try {
            if (currentTransferItem != null) {
                currentTransferItem.moveToDir(outboxFailed, e);
            }
            else {
                log.warn("Failed, but no transferItem was set!");
            }
        }
        catch (IOException ioe) {
            log.error("Unable to move transfer item to failed outbox", ioe);
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
}
