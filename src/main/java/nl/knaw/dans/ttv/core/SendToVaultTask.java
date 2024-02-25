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

import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.datavault.client.api.ImportCommandDto;
import nl.knaw.dans.datavault.client.resources.DefaultApi;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.db.TransferItemDao;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.FileUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;

@Slf4j
@ToString
@AllArgsConstructor
public class SendToVaultTask implements Runnable {
    private final FileService fileService;
    private final TransferItemService transferItemService;
    private final Path filePath;
    private final Path currentBatchPath;
    private final long threshold;
    private final Path outbox;
    private final DefaultApi vaultApi;

    @Override
    public void run() {
        try {
            processFile(this.filePath);
        }
        catch (Exception e) {
            log.error("Unable to send file to vault", e);
            try {
                fileService.rejectFile(this.filePath, e);
            }
            catch (Exception e1) {
                log.error("I/O error while rejecting file", e1);
            }
        }
    }

    private void processFile(Path path) throws Exception {
        /*
         * SendToVaultTasks are executed on a single thread executor, so we don't have to worry about concurrent access to the currentBatchPath.
         */
        transferItemService.findByDveFilename(path.getFileName().toString())
            .ifPresentOrElse(
                transferItem -> processTransferItem(path, transferItem),
                () -> log.error("No TransferItem found for path '{}'", path)
            );
    }

    private void processTransferItem(Path path, TransferItem transferItem) {
        try {
            var nbn = transferItem.getNbn();
            var creationTime = transferItem.getCreationTime();
            Instant instant = creationTime.atZoneSameInstant(ZoneId.systemDefault()).toInstant();
            long epochMilli = instant.toEpochMilli();
            addToObjectImportDirectory(path, epochMilli, this.currentBatchPath.resolve(nbn));
            if (fileService.getPathSize(this.currentBatchPath) > this.threshold) {
                var batch = this.outbox.resolve("batch-" + System.currentTimeMillis());
                log.info("Threshold reached, sending batch {} to vault", batch);
                FileUtils.moveDirectory(this.currentBatchPath.toFile(), batch.toFile());
                sendBatchToVault(batch);
                Files.createDirectories(this.currentBatchPath);
            }
        }
        catch (Exception e) {
            log.error("Unable to update TransferItem for path '{}'", path, e);
            try {
                fileService.rejectFile(path, e);
            }
            catch (Exception e1) {
                log.error("I/O error while rejecting file", e1);
            }
        }
    }

    private void addToObjectImportDirectory(Path dvePath, long epochMilli, Path objectImportDirectory) throws IOException {
        fileService.ensureDirectoryExists(objectImportDirectory);
        try {
            extractZipFile(dvePath, objectImportDirectory.resolve(String.valueOf(epochMilli)));
            Files.delete(dvePath);
        }
        catch (IOException e) {
            log.error("Unable to extract zip file", e);
            fileService.rejectFile(dvePath, e);
        }
    }

    private void extractZipFile(Path zipFilePath, Path outputDirectory) throws IOException {
        try (ZipFile zipFile = new ZipFile(zipFilePath.toFile())) {
            zipFile.getEntries().asIterator().forEachRemaining(entry -> {
                try (InputStream input = zipFile.getInputStream(entry)) {
                    Path outputPath = outputDirectory.resolve(entry.getName());
                    try (OutputStream output = new FileOutputStream(outputPath.toFile())) {
                        input.transferTo(output);
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private void sendBatchToVault(Path batchPath) {
        try {
            var importCommand = new ImportCommandDto()
                .path(batchPath.toString());
            vaultApi.importsPost(importCommand);
        }
        catch (Exception e) {
            log.error("Unable to send batch to vault", e);
            try {
                fileService.rejectFile(batchPath, e);
            }
            catch (Exception e1) {
                log.error("I/O error while rejecting batch", e1);
            }
        }
    }
}
