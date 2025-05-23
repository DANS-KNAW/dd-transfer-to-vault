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
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.util.ZipUtil;
import nl.knaw.dans.transfer.client.DataVaultClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.commons.io.FileUtils.moveDirectory;
import static org.apache.commons.io.FileUtils.sizeOfDirectory;

@Slf4j
@ToString
@AllArgsConstructor
public class SendToVaultTask implements Runnable {
    private final Path dve;
    private final Path currentBatchWorkDir;
    private final Path dataVaultBatchRoot;
    private final DataSize batchThreshold;
    private final DataSize layerThreshold;
    private final Path outboxProcessed;
    private final Path outboxFailed;
    private final DataVaultClient dataVaultClient;

    @Override
    public void run() {
        TransferItem transferItem = null;
        try {
            transferItem = new TransferItem(dve);
            addToObjectImportDirectory(dve, transferItem.getOcflObjectVersion(), this.currentBatchWorkDir.resolve(transferItem.getNbn()));
            createNewLayerIfLayerThresholdReached();
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
        FileUtils.ensureDirectoryExists(objectImportDirectory);
        var versionDirectory = objectImportDirectory.resolve("v" + ocflObjectVersionNumber);
        log.debug("Extracting DVE {} to {}", dvePath, versionDirectory);
        ZipUtil.extractZipFile(dvePath, versionDirectory);
    }

    private void createNewLayerIfLayerThresholdReached() {
        if (dataVaultClient.getTopLayerSize() > layerThreshold.toBytes()) {
            log.info("Layer threshold ({}) reached, creating new layer in Data Vault", this.layerThreshold);
            var layerStatusDto = dataVaultClient.createNewLayer();
            log.info("New layer created in Data Vault; id = {}", layerStatusDto.getLayerId());
        }
        else {
            log.debug("Layer threshold not reached, current size: {}", dataVaultClient.getTopLayerSize());
        }
    }

    private void importIfBatchThresholdReached() throws IOException {
        if (sizeOfDirectory(this.currentBatchWorkDir.toFile()) > this.batchThreshold.toBytes()) {
            var batch = dataVaultBatchRoot.resolve("batch-" + System.currentTimeMillis());
            log.info("Batch threshold ({}) reached, sending batch {} to Data Vault", this.batchThreshold, batch);
            log.info("Moving current batch directory {} to {}", currentBatchWorkDir, batch);
            moveDirectory(currentBatchWorkDir.toFile(), batch.toFile());
            dataVaultClient.sendBatchToVault(batch);
            log.info("Recreating current batch directory");
            Files.createDirectories(this.currentBatchWorkDir);
        }
    }
}
