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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.transfer.client.DataVaultClient;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@RequiredArgsConstructor
@Slf4j
public class SendToVaultFlushTask implements Runnable {
    private final Path currentBatchWorkDir;
    private final Path dataVaultBatchRoot;
    private final DataVaultClient dataVaultClient;

    @Override
    public void run() {
        if (isDirEmpty(currentBatchWorkDir)) {
            log.info("FLUSH: current batch is empty; nothing to do...");
        }
        else {
            sendWorkToVault();
        }
    }

    private boolean isDirEmpty(Path dir) {
        try (var stream = Files.list(dir)) {
            return stream.findAny().isEmpty();
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void sendWorkToVault() {
        try {
            var batch = dataVaultBatchRoot.resolve("batch-" + System.currentTimeMillis());
            log.info("FLUSH: Moving current batch directory {} to {}", currentBatchWorkDir, batch);
            FileUtils.moveDirectory(currentBatchWorkDir.toFile(), batch.toFile());
            log.info("FLUSH: Calling dd-data-vault to ingest batch {}", batch);
            dataVaultClient.sendBatchToVault(batch);
            log.info("FLUSH: Recreating empty current batch directory");
            Files.createDirectories(this.currentBatchWorkDir);
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
