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
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.util.ZipUtil;
import nl.knaw.dans.transfer.client.DataVaultClient;
import nl.knaw.dans.transfer.config.CustomPropertyConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.apache.commons.io.FileUtils.moveDirectory;
import static org.apache.commons.io.FileUtils.sizeOfDirectory;

@Slf4j
@ToString
@RequiredArgsConstructor
public class SendToVaultTask implements Runnable {
    private final Path dve;
    private final Path currentBatchWorkDir;
    private final Path dataVaultBatchRoot;
    private final DataSize batchThreshold;
    private final Path outboxProcessed;
    private final Path outboxFailed;
    private final DataVaultClient dataVaultClient;
    private final String defaultMessage;
    private final Map<String, CustomPropertyConfig> customProperties;
    private final FileService fileService;

    private TransferItem transferItem;

    @Override
    public void run() {
        try {
            transferItem = new TransferItem(dve);
            addToObjectImportDirectory(dve, transferItem.getOcflObjectVersion(), this.currentBatchWorkDir.resolve(transferItem.getNbn()));
            log.info("Added {} to current batch", dve.getFileName());
            importIfBatchThresholdReached();
            transferItem.moveToDir(outboxProcessed);
        }
        catch (Exception e) {
            log.error("Failed to process DVE {}: {}", dve, e.getMessage());
            if (transferItem != null) {
                try {
                    transferItem.moveToDir(outboxFailed, e);
                }
                catch (IOException ioException) {
                    log.error("Unable to move file to outbox failed", ioException);
                }
            }
        }
    }

    private void addToObjectImportDirectory(Path dvePath, int ocflObjectVersionNumber, Path objectImportDirectory) throws IOException {
        fileService.ensureDirectoryExists(objectImportDirectory);
        var versionDirectory = objectImportDirectory.resolve("v" + ocflObjectVersionNumber);
        log.debug("Extracting DVE {} to {}", dvePath, versionDirectory);
        ZipUtil.extractZipFile(dvePath, versionDirectory);
        createVersionInfoProperties(versionDirectory, transferItem.getContactName(), transferItem.getContactEmail(), defaultMessage);
    }

    private void createVersionInfoProperties(Path versionDirectory, String user, String email, String message) throws IOException {
        var versionInfoFile = versionDirectory.resolveSibling(versionDirectory.getFileName().toString() + ".properties");
        log.debug("Creating version info properties file at {}", versionInfoFile);
        var sb = new StringBuilder();
        sb.append(String.format("""
            user.name=%s
            user.email=%s
            message=%s
            """, user, email, message));

        if (customProperties != null) {
            for (var entry : customProperties.entrySet()) {
                var name = entry.getKey();
                var config = entry.getValue();
                var value = config.getValue(transferItem);

                value.ifPresent(v -> {
                    if (!v.isBlank()) {
                        sb.append(String.format("custom.%s=%s\n", name, v));
                    }
                });
            }
        }

        Files.writeString(versionInfoFile, sb.toString(), StandardCharsets.UTF_8);
    }


    private void importIfBatchThresholdReached() throws IOException {
        if (sizeOfDirectory(this.currentBatchWorkDir.toFile()) > this.batchThreshold.toBytes()) {
            var batch = dataVaultBatchRoot.resolve("batch-" + System.currentTimeMillis());
            log.info("Batch threshold ({}) reached, sending batch {} to Data Vault", this.batchThreshold, batch);
            log.info("Moving current batch directory {} to {}", currentBatchWorkDir, batch);
            moveDirectory(currentBatchWorkDir.toFile(), batch.toFile());
            dataVaultClient.sendBatchToVault(batch);
            log.info("Recreating empty current batch directory");
            Files.createDirectories(this.currentBatchWorkDir);
        }
    }
}
