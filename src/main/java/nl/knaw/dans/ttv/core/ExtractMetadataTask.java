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
import nl.knaw.dans.ttv.client.GmhClient;
import nl.knaw.dans.ttv.client.VaultCatalogClient;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReader;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.core.service.TransferItemValidator;

import java.io.IOException;
import java.nio.file.Path;

@Slf4j
@ToString
@AllArgsConstructor
public class ExtractMetadataTask implements Runnable {
    private final Path filePath;
    private final Path outbox;
    private final TransferItemService transferItemService;
    private final TransferItemMetadataReader metadataReader;
    private final FileService fileService;
    private final TransferItemValidator transferItemValidator;
    private final VaultCatalogClient vaultCatalogClient;
    private final GmhClient gmhClient;

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
        catch (Throwable e) {
            log.error("Unexpected error while processing file '{}'", this.filePath, e);

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
        log.debug("Processing file '{}' with TransferItem {}", path, transferItem);

        if (transferItem.getTransferStatus() != TransferItem.TransferStatus.METADATA_EXTRACTED
            && transferItem.getTransferStatus() != TransferItem.TransferStatus.COLLECTED) {
            throw new InvalidTransferItemException(String.format("TransferItem already exists but with an unexpected status: %s", transferItem));
        }

        // we only expect items with status COLLECTED, but if they are already METADATA_EXTRACTED we
        // can just read the metadata again and update the TransferItem before moving
        var fileContentAttributes = metadataReader.getFileContentAttributes(path);
        log.debug("Retrieved file content attributes: {}", fileContentAttributes);

        // apply content attributes and validate the transfer item
        transferItemService.addMetadata(transferItem, fileContentAttributes);
        transferItemValidator.validateTransferItem(transferItem);

        var newPath = outbox.resolve(transferItem.getDveFilename());

        vaultCatalogClient.registerOcflObjectVersion(transferItem);

        if (transferItem.getOcflObjectVersionNumber() == 1) {
            gmhClient.registerNbn(transferItem);
        }

        log.debug("Updated file metadata, moving file '{}' to '{}'", path, newPath);
        transferItemService.moveTransferItem(transferItem, TransferItem.TransferStatus.METADATA_EXTRACTED, newPath);

        fileService.moveFile(path, newPath);
    }

    public TransferItem getTransferItem(Path path) throws InvalidTransferItemException {
        var filenameAttributes = metadataReader.getFilenameAttributes(path);
        log.debug("Retrieved filename attributes: {}", filenameAttributes);

        return transferItemService.getTransferItemByFilenameAttributes(filenameAttributes)
            .orElseThrow(() -> new InvalidTransferItemException(String.format("no TransferItem found for filename attributes %s", filenameAttributes)));
    }
}
