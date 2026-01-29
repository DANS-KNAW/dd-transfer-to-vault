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

import io.dropwizard.util.DataSize;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.util.ZipUtil;
import nl.knaw.dans.lib.util.healthcheck.DependenciesReadyCheck;
import nl.knaw.dans.transfer.client.DataVaultClient;
import nl.knaw.dans.transfer.config.CustomPropertyConfig;
import nl.knaw.dans.transfer.health.HealthChecks;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static org.apache.commons.io.FileUtils.moveDirectory;
import static org.apache.commons.io.FileUtils.sizeOfDirectory;

@Slf4j
@ToString
public class SendToVaultTask extends SourceDirItemProcessor implements Runnable {
    public static final String NEWLINE_TAB_REGEX = "[\\n\\t\\r]";
    private final Path targetNbnDir;
    private final Path currentBatchWorkDir;
    private final Path dataVaultBatchRoot;
    private final DataSize batchThreshold;
    private final Path outboxProcessed;
    private final Path outboxFailed;
    private final DataVaultClient dataVaultClient;
    private final String defaultMessage;
    private final Map<String, CustomPropertyConfig> customProperties;
    private final FileService fileService;
    private final DependenciesReadyCheck readyCheck;

    private TransferItem currentTransferItem;

    public SendToVaultTask(Path srcDir, Path currentBatchWorkDir, Path dataVaultBatchRoot, DataSize batchThreshold, Path outboxProcessed, Path outboxFailed,
        DataVaultClient dataVaultClient, String defaultMessage, Map<String, CustomPropertyConfig> customProperties, FileService fileService, DependenciesReadyCheck readyCheck,
        long delayBetweenProcessingRounds) {
        super(srcDir, "DVE", new DveFileFilter().toPredicate(), CreationTimeComparator.getInstance(), fileService, delayBetweenProcessingRounds);
        this.targetNbnDir = srcDir;
        this.currentBatchWorkDir = currentBatchWorkDir;
        this.dataVaultBatchRoot = dataVaultBatchRoot;
        this.batchThreshold = batchThreshold;
        this.outboxProcessed = outboxProcessed;
        this.outboxFailed = outboxFailed;
        this.dataVaultClient = dataVaultClient;
        this.defaultMessage = defaultMessage;
        this.customProperties = customProperties;
        this.fileService = fileService;
        this.readyCheck = readyCheck;
    }

    @Override
    public void run() {
        log.debug("Started SendToVaultTask for {}", targetNbnDir);
        readyCheck.waitUntilReady(HealthChecks.FILESYSTEM_PERMISSIONS, HealthChecks.FILESYSTEM_FREE_SPACE, HealthChecks.DATA_VAULT);
        log.debug("Readycheck complete");

        processUntilRemoved();
    }

    @Override
    protected void processItem(@NonNull Path item) throws IOException {
        log.debug("Processing DVE {}", item);
        currentTransferItem = new TransferItem(item, fileService);
        addToObjectImportDirectory(item, currentTransferItem.getOcflObjectVersion(), this.currentBatchWorkDir.resolve(currentTransferItem.getNbn()));
        log.info("Added {} to current batch", item.getFileName());
        importIfBatchThresholdReached();
        currentTransferItem.moveToDir(outboxProcessed);
    }

    @Override
    protected void rejectCurrentItem(@NonNull IllegalArgumentException e) {
        // There is no validation step for SendToVaultTask, so any exception is considered a failure.
        failCurrentItem(e);
    }

    @Override
    protected void failCurrentItem(@NonNull Exception e) {
        log.error("Failed to process item: {}", e.getMessage());
        if (currentTransferItem != null) {
            try {
                currentTransferItem.moveToErrorBox(outboxFailed, e);
            }
            catch (IOException ioException) {
                log.error("Unable to move file to outbox failed", ioException);
            }
        }
    }

    private void addToObjectImportDirectory(@NonNull Path dvePath, int ocflObjectVersionNumber, @NonNull Path objectImportDirectory) throws IOException {
        fileService.ensureDirectoryExists(objectImportDirectory);
        var versionDirectory = objectImportDirectory.resolve("v" + ocflObjectVersionNumber);
        log.debug("Extracting DVE {} to {}", dvePath, versionDirectory);
        ZipUtil.extractZipFile(dvePath, versionDirectory);
        createVersionInfoProperties(versionDirectory, currentTransferItem.getContactName(), currentTransferItem.getContactEmail(), defaultMessage);
    }

    void createVersionInfoProperties(@NonNull Path versionDirectory, @NonNull String user, @NonNull String email, @NonNull String message) throws IOException {
        var versionInfoFile = versionDirectory.resolveSibling(versionDirectory.getFileName().toString() + ".properties");
        log.debug("Creating version info properties file at {}", versionInfoFile);

        var props = new Properties();
        props.setProperty("user.name", user.replaceAll(NEWLINE_TAB_REGEX, "").trim());
        props.setProperty("user.email", email.replaceAll(NEWLINE_TAB_REGEX, "").trim());
        props.setProperty("message", message);

        if (customProperties != null) {
            for (var entry : customProperties.entrySet()) {
                var name = entry.getKey();
                var config = entry.getValue();
                var value = config.getValue(currentTransferItem);

                value.ifPresent(v -> {
                    if (!v.isBlank()) {
                        props.setProperty("custom." + name, v);
                    }
                });
            }
        }

        try (var os = fileService.newOutputStream(versionInfoFile)) {
            props.store(os, null);
        }
    }

    private void importIfBatchThresholdReached() throws IOException {
        if (sizeOfDirectory(this.currentBatchWorkDir.toFile()) > this.batchThreshold.toBytes()) {
            var batch = dataVaultBatchRoot.resolve("batch-" + System.currentTimeMillis());
            log.info("Batch threshold ({}) reached, sending batch {} to Data Vault", this.batchThreshold, batch);
            log.info("Moving current batch directory {} to {}", currentBatchWorkDir, batch);
            moveDirectory(currentBatchWorkDir.toFile(), batch.toFile());
            log.info("Recreating empty current batch directory");
            fileService.createDirectory(this.currentBatchWorkDir);
            log.info("Calling Data Vault to process batch {}", batch);
            dataVaultClient.sendBatchToVault(batch);
        }
    }
}
