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
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReader;
import nl.knaw.dans.ttv.core.service.TransferItemService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

@Slf4j
@ToString
@AllArgsConstructor
public class CollectTask implements Runnable {
    private final Path filePath;
    private final Path outbox;
    private final String datastationName;
    private final TransferItemService transferItemService;
    private final TransferItemMetadataReader metadataReader;
    private final FileService fileService;

    @Override
    public void run() {
        try {
            processFile(this.filePath);
        }
        catch (IOException | InvalidTransferItemException e) {
            log.error("Unable to create TransferItem for path '{}'", this.filePath, e);

            try {
                moveFileToErrorBox(this.filePath, e);
            }
            catch (IOException error) {
                log.error("Tried to move file to dead-letter box, but failed", error);
            }
        }
        finally {
            log.info("Cleaning up XML file associated with '{}'", this.filePath);
            cleanUpXmlFile(this.filePath);
        }
    }

    void processFile(Path path) throws IOException, InvalidTransferItemException {
        var filenameAttributes = metadataReader.getFilenameAttributes(path);
        log.debug("Filename attributes: {}", filenameAttributes);

        var filesystemAttributes = metadataReader.getFilesystemAttributes(path);
        log.debug("Filesystem attributes: {}", filesystemAttributes);

        var optExistingTransferItem = transferItemService.getTransferItemByFilenameAttributes(filenameAttributes)
            .filter(item -> Objects.equals(item.getBagSha256Checksum(), filesystemAttributes.getChecksum()));

        TransferItem transferItem;
        if (optExistingTransferItem.isPresent()) {
            checkStatusOfExistingTransferItem(optExistingTransferItem.get());
            transferItem = optExistingTransferItem.get();
        } else {
            transferItem = transferItemService.createTransferItem(datastationName, filenameAttributes, filesystemAttributes, null);
        }
        log.debug("Moving file '{}' to outbox '{}'", path, this.outbox);
        moveFileToOutbox(transferItem, path, this.outbox);
    }

    private void checkStatusOfExistingTransferItem(TransferItem transferItem) throws InvalidTransferItemException {
        if (transferItem.getTransferStatus() != TransferItem.TransferStatus.COLLECTED) {
            throw new InvalidTransferItemException(
                String.format("TransferItem exists already, but does not have expected status of COLLECTED: %s", transferItem)
            );
        }
    }

    void moveFileToOutbox(TransferItem transferItem, Path filePath, Path outboxPath) throws IOException {
        var newPath = outboxPath.resolve(transferItem.getDveFilename());
        log.debug("filePath is '{}', newPath is '{}'", filePath, newPath);

        log.debug("Updating database state for item {} with new path '{}'", transferItem, outboxPath);
        transferItemService.moveTransferItem(transferItem, TransferItem.TransferStatus.COLLECTED, newPath);

        fileService.moveFileAtomically(filePath, newPath);
    }

    void moveFileToErrorBox(Path path, Exception exception) throws IOException {
        fileService.rejectFile(path, exception);
    }

    void cleanUpXmlFile(Path path) {
        metadataReader.getAssociatedXmlFile(path).ifPresent(p -> {
            log.debug("Deleting file '{}'", p);
            try {
                fileService.deleteFile(p);
            }
            catch (IOException e) {
                log.error("Unable to delete XML file associated with file '{}' (filename: '{}')", path, p, e);
            }
        });
    }
}
