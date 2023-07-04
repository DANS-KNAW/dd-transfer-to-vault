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

import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReader;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.core.service.TransferItemValidator;
import nl.knaw.dans.ttv.db.TransferItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

public class ExtractMetadataTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ExtractMetadataTask.class);

    private final Path filePath;
    private final Path outbox;
    private final TransferItemService transferItemService;
    private final TransferItemMetadataReader metadataReader;
    private final FileService fileService;
    private final TransferItemValidator transferItemValidator;
    private final VaultCatalogRepository vaultCatalogRepository;

    public ExtractMetadataTask(Path filePath, Path outbox, TransferItemService transferItemService,
        TransferItemMetadataReader metadataReader, FileService fileService, TransferItemValidator transferItemValidator, VaultCatalogRepository vaultCatalogRepository) {
        this.filePath = filePath;
        this.outbox = outbox;
        this.transferItemService = transferItemService;
        this.metadataReader = metadataReader;
        this.fileService = fileService;
        this.transferItemValidator = transferItemValidator;
        this.vaultCatalogRepository = vaultCatalogRepository;
    }

    @Override
    public void run() {
        try {
            processFile(this.filePath);
        }
        catch (IOException | InvalidTransferItemException e) {
            log.error("Unable to create TransferItem for path '{}'", this.filePath, e);

            try {
                fileService.rejectFile(this.filePath, e);
            }
            catch (IOException ex) {
                log.error("Unable to reject file", ex);
            }
        }
    }

    void processFile(Path path) throws IOException, InvalidTransferItemException {
        var transferItem = getTransferItem(path);
        log.info("Processing file '{}' with TransferItem {}", path, transferItem);

        if (
            transferItem.getTransferStatus() != TransferItem.TransferStatus.METADATA_EXTRACTED
                && transferItem.getTransferStatus() != TransferItem.TransferStatus.COLLECTED
        ) {
            throw new InvalidTransferItemException(String.format("TransferItem already exists but with an unexpected status: %s", transferItem));
        }

        // we only expect items with status COLLECTED, but if they are already METADATA_EXTRACTED we
        // can just read the metadata again and update the TransferItem before moving
        var fileContentAttributes = metadataReader.getFileContentAttributes(path);
        log.trace("Received file content attributes: {}", fileContentAttributes);

        // apply content attributes and validate the transfer item
        var updatedTransferItem = transferItemService.addMetadata(transferItem, fileContentAttributes);
        transferItemValidator.validateTransferItem(updatedTransferItem);

        var newPath = outbox.resolve(filePath.getFileName());

        vaultCatalogRepository.registerOcflObjectVersion(transferItem);

        transferItemService.moveTransferItem(updatedTransferItem, TransferItem.TransferStatus.METADATA_EXTRACTED, newPath);

        log.info("Updated file metadata, moving file '{}' to '{}'", path, newPath);
        fileService.moveFile(path, newPath);
    }

    public TransferItem getTransferItem(Path path) throws InvalidTransferItemException {
        var filenameAttributes = metadataReader.getFilenameAttributes(path);
        log.trace("Received filename attributes: {}", filenameAttributes);

        return transferItemService.getTransferItemByFilenameAttributes(filenameAttributes)
            .orElseThrow(() -> new InvalidTransferItemException(String.format("no TransferItem found for filename attributes %s", filenameAttributes)));
    }

    @Override
    public String toString() {
        return "MetadataTask{" +
            "filePath=" + filePath +
            ", outbox=" + outbox +
            '}';
    }
}
