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
import nl.knaw.dans.ttv.db.TransferItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

public class CollectTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(CollectTask.class);

    private final Path filePath;
    private final Path outbox;
    private final String datastationName;
    private final TransferItemService transferItemService;
    private final TransferItemMetadataReader metadataReader;
    private final FileService fileService;

    public CollectTask(Path filePath, Path outbox, String datastationName, TransferItemService transferItemService,
        TransferItemMetadataReader metadataReader, FileService fileService) {
        this.filePath = filePath;
        this.outbox = outbox;
        this.datastationName = datastationName;
        this.transferItemService = transferItemService;
        this.metadataReader = metadataReader;
        this.fileService = fileService;
    }

    @Override
    public void run() {
        try {
            processFile(this.filePath);
        }
        catch (IOException | InvalidTransferItemException e) {
            log.error("unable to create TransferItem for path '{}'", this.filePath, e);
            // TODO move to deadletter box
        }
    }

    void processFile(Path path) throws IOException, InvalidTransferItemException {
        var transferItem = createOrGetTransferItem(path);

        if (transferItem.getTransferStatus() != TransferItem.TransferStatus.CREATED) {
            throw new InvalidTransferItemException(
                String.format("TransferItem exists already, but does not have expected status of CREATED: %s", transferItem));
        }

        moveFileToOutbox(transferItem, path, this.outbox);
        cleanUpXmlFile(this.filePath);
    }

    public TransferItem createOrGetTransferItem(Path path) throws InvalidTransferItemException {
        var filenameAttributes = metadataReader.getFilenameAttributes(path);
        log.trace("received filename attributes: {}", filenameAttributes);

        var transferItem = transferItemService.getTransferItemByFilenameAttributes(filenameAttributes);
        log.trace("TransferItem from database: {}", transferItem);

        if (transferItem.isPresent()) {
            return transferItem.get();
        }

        var filesystemAttributes = metadataReader.getFilesystemAttributes(path);

        log.debug("no existing TransferItem found, creating new one with attributes {} and {}",
            filenameAttributes, filesystemAttributes);

        return transferItemService.createTransferItem(datastationName, filenameAttributes, filesystemAttributes);
    }

    public void moveFileToOutbox(TransferItem transferItem, Path filePath, Path outboxPath) throws IOException {
        log.trace("filePath is '{}', outboxPath is '{}'", filePath, outboxPath);
        var newPath = outboxPath.resolve(filePath.getFileName());

        log.trace("updating database state for item {} with new path '{}'", transferItem, newPath);
        transferItemService.moveTransferItem(transferItem, TransferItem.TransferStatus.CREATED, newPath);

        log.info("moving file '{}' to location '{}'", filePath, newPath);
        fileService.moveFileAtomically(filePath, newPath);
    }

    public void cleanUpXmlFile(Path path) {
        metadataReader.getAssociatedXmlFile(path)
            .ifPresent(p -> {
                try {
                    fileService.deleteFile(p);
                }
                catch (IOException e) {
                    log.error("unable to delete XML file associated with file '{}' (filename: '{}')", path, p, e);
                }
            });
    }
}
