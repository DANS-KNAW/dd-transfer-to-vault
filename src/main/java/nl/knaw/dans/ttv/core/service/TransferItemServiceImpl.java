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
package nl.knaw.dans.ttv.core.service;

import io.dropwizard.hibernate.UnitOfWork;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ttv.core.InvalidTransferItemException;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.ttv.core.domain.FileContentAttributes;
import nl.knaw.dans.ttv.core.domain.FilenameAttributes;
import nl.knaw.dans.ttv.core.domain.FilesystemAttributes;
import nl.knaw.dans.ttv.db.TransferItemDao;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

@Slf4j
@AllArgsConstructor
public class TransferItemServiceImpl implements TransferItemService {
    private final TransferItemDao transferItemDao;

    @Override
    @UnitOfWork
    public TransferItem createTransferItem(
        String datastationName,
        FilenameAttributes filenameAttributes,
        FilesystemAttributes filesystemAttributes
    ) throws InvalidTransferItemException {
        var transferItem = new TransferItem();

        transferItem.setTransferStatus(TransferItem.TransferStatus.COLLECTED);
        transferItem.setDatastation(datastationName);

        // filename attributes
        transferItem.setDveFilePath(filenameAttributes.getDveFilePath());
        transferItem.setDveFilename(filenameAttributes.getDveFilename());
        transferItem.setOcflObjectVersionNumber(filenameAttributes.getOcflObjectVersionNumber());

        // filesystem attributes
        transferItem.setCreationTime(filesystemAttributes.getCreationTime());
        transferItem.setBagSize(filesystemAttributes.getBagSize());
        transferItem.setBagSha256Checksum(filesystemAttributes.getChecksum());

        // check if an item with this ID already exists
        var existing = transferItemDao.findByIdentifier(transferItem.getDveFilename())
            .filter(item -> Objects.equals(item.getBagSha256Checksum(), transferItem.getBagSha256Checksum()));

        if (existing.isPresent()) {
            throw new InvalidTransferItemException(
                String.format("TransferItem with dveFilename=%s and identical checksums already exists in database",
                    transferItem.getDveFilename()
                )
            );
        }

        transferItemDao.save(transferItem);
        return transferItem;
    }

    @Override
    @UnitOfWork
    public TransferItem moveTransferItem(TransferItem transferItem, TransferItem.TransferStatus newStatus, Path newPath) {
        transferItem.setDveFilePath(newPath.toString());
        transferItem.setTransferStatus(newStatus);
        transferItemDao.merge(transferItem);

        return transferItem;
    }

    @Override
    @UnitOfWork
    public Optional<TransferItem> getTransferItemByFilenameAttributes(FilenameAttributes filenameAttributes) {
        if (filenameAttributes == null) {
            return Optional.empty();
        }
        return transferItemDao.findByIdentifier(filenameAttributes.getDveFilename());
    }

    @Override
    public TransferItem addMetadata(@NonNull TransferItem transferItem, @NonNull FileContentAttributes fileContentAttributes) {
        // file content attributes
        transferItem.setDataversePid(fileContentAttributes.getDataversePid());
        transferItem.setDataversePidVersion(fileContentAttributes.getDataversePidVersion());
        transferItem.setBagId(fileContentAttributes.getBagId());
        transferItem.setNbn(fileContentAttributes.getNbn());
        transferItem.setMetadata(fileContentAttributes.getMetadata());
        transferItem.setOtherId(fileContentAttributes.getOtherId());
        transferItem.setOtherIdVersion(fileContentAttributes.getOtherIdVersion());
        transferItem.setSwordToken(fileContentAttributes.getSwordToken());
        transferItem.setDataSupplier(fileContentAttributes.getDataSupplier());
        return transferItem;
    }
}
