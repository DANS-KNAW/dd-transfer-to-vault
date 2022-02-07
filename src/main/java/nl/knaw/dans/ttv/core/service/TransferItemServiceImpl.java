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
import nl.knaw.dans.ttv.core.InvalidTransferItemException;
import nl.knaw.dans.ttv.core.dto.FileContentAttributes;
import nl.knaw.dans.ttv.core.dto.FilenameAttributes;
import nl.knaw.dans.ttv.core.dto.FilesystemAttributes;
import nl.knaw.dans.ttv.db.TransferItem;
import nl.knaw.dans.ttv.db.TransferItemDAO;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class TransferItemServiceImpl implements TransferItemService {
    private final TransferItemDAO transferItemDAO;

    public TransferItemServiceImpl(TransferItemDAO transferItemDAO) {
        this.transferItemDAO = transferItemDAO;
    }

    @Override
    @UnitOfWork
    public List<TransferItem> findByStatus(TransferItem.TransferStatus status) {
        Objects.requireNonNull(status, "status cannot be null");
        return transferItemDAO.findByStatus(status);
    }

    @Override
    @UnitOfWork
    public List<TransferItem> findByTarId(String id) {
        Objects.requireNonNull(id, "id cannot be null");
        return transferItemDAO.findAllByTarId(id);
    }

    @Override
    @UnitOfWork
    public TransferItem createTransferItem(String datastationName, FilenameAttributes filenameAttributes, FilesystemAttributes filesystemAttributes, FileContentAttributes fileContentAttributes)
        throws InvalidTransferItemException {
        var transferItem = new TransferItem();

        transferItem.setTransferStatus(TransferItem.TransferStatus.COLLECTED);
        transferItem.setQueueDate(LocalDateTime.now());
        transferItem.setDatasetDvInstance(datastationName);

        // filename attributes
        transferItem.setDveFilePath(filenameAttributes.getDveFilePath());
        transferItem.setDatasetPid(filenameAttributes.getDatasetPid());
        transferItem.setVersionMajor(filenameAttributes.getVersionMajor());
        transferItem.setVersionMinor(filenameAttributes.getVersionMinor());

        // filesystem attributes
        transferItem.setCreationTime(filesystemAttributes.getCreationTime());
        transferItem.setBagChecksum(filesystemAttributes.getBagChecksum());
        transferItem.setBagSize(filesystemAttributes.getBagSize());

        // file content attributes
        transferItem.setDatasetVersion(fileContentAttributes.getDatasetVersion());
        transferItem.setBagId(fileContentAttributes.getBagId());
        transferItem.setNbn(fileContentAttributes.getNbn());
        transferItem.setOaiOre(fileContentAttributes.getOaiOre());
        transferItem.setPidMapping(fileContentAttributes.getPidMapping());

        // check if an item with this ID already exists
        var existing = transferItemDAO.findByDatasetPidAndVersion(transferItem.getDatasetPid(), transferItem.getVersionMajor(), transferItem.getVersionMinor());

        if (existing.isPresent()) {
            throw new InvalidTransferItemException(
                String.format("TransferItem with datasetPid=%s, versionMajor=%s, versionMinor=%s already exists in database", transferItem.getDatasetPid(), transferItem.getVersionMajor(),
                    transferItem.getVersionMinor()));

        }

        transferItemDAO.save(transferItem);

        return transferItem;
    }

    @Override
    @UnitOfWork
    public TransferItem createTransferItem(String datastationName, FilenameAttributes filenameAttributes) throws InvalidTransferItemException {

        // check if an item with this ID already exists
        var existing = transferItemDAO.findByDatasetPidAndVersion(
            filenameAttributes.getDatasetPid(),
            filenameAttributes.getVersionMajor(),
            filenameAttributes.getVersionMinor()
        );

        if (existing.isPresent()) {
            throw new InvalidTransferItemException(
                String.format("TransferItem with datasetPid=%s, versionMajor=%s, versionMinor=%s already exists in database",
                    filenameAttributes.getDatasetPid(),
                    filenameAttributes.getVersionMajor(),
                    filenameAttributes.getVersionMinor()
                )
            );
        }

        var transferItem = new TransferItem();

        transferItem.setTransferStatus(TransferItem.TransferStatus.CREATED);
        transferItem.setQueueDate(LocalDateTime.now());
        transferItem.setDatasetDvInstance(datastationName);

        // filename attributes
        transferItem.setDveFilePath(filenameAttributes.getDveFilePath());
        transferItem.setDatasetPid(filenameAttributes.getDatasetPid());
        transferItem.setVersionMajor(filenameAttributes.getVersionMajor());
        transferItem.setVersionMinor(filenameAttributes.getVersionMinor());

        transferItemDAO.save(transferItem);
        return transferItem;
    }

    @Override
    @UnitOfWork
    public TransferItem moveTransferItem(TransferItem transferItem, TransferItem.TransferStatus newStatus, Path newPath) {
        transferItem.setDveFilePath(newPath.toString());
        transferItem.setTransferStatus(newStatus);
        transferItemDAO.merge(transferItem);
        return transferItem;
    }

    @Override
    @UnitOfWork
    public void saveAll(List<TransferItem> transferItems) {
        for (var transferItem : transferItems) {
            transferItemDAO.merge(transferItem);
        }
    }

    @Override
    @UnitOfWork
    public void updateToCreatedForTarId(String id) {
        transferItemDAO.updateStatusByTar(id, TransferItem.TransferStatus.OCFLTARCREATED);
    }

    @Override
    @UnitOfWork
    public List<String> stageAllTarsToBeConfirmed() {
        var results = transferItemDAO.findAllTarsToBeConfirmed();

        for (var item : results) {
            item.setConfirmCheckInProgress(true);
        }

        saveAll(results);

        return results.stream().map(TransferItem::getAipsTar).distinct().collect(Collectors.toList());
    }

    @Override
    @UnitOfWork
    public void updateCheckingProgressResults(String id, TransferItem.TransferStatus status) {
        transferItemDAO.updateCheckingProgressResults(id, status);
    }

    @Override
    @UnitOfWork
    public Optional<TransferItem> getTransferItemByFilenameAttributes(FilenameAttributes filenameAttributes) {
        return transferItemDAO.findByDatasetPidAndVersion(
            filenameAttributes.getDatasetPid(),
            filenameAttributes.getVersionMajor(),
            filenameAttributes.getVersionMinor()
        );
    }

    @Override
    @UnitOfWork
    public TransferItem addMetadataAndMoveFile(TransferItem transferItem, FilesystemAttributes filesystemAttributes, FileContentAttributes fileContentAttributes, TransferItem.TransferStatus status,
        Path newPath) {

        transferItem.setTransferStatus(status);
        transferItem.setDveFilePath(newPath.toString());

        // filesystem attributes
        transferItem.setCreationTime(filesystemAttributes.getCreationTime());
        transferItem.setBagChecksum(filesystemAttributes.getBagChecksum());
        transferItem.setBagSize(filesystemAttributes.getBagSize());

        // file content attributes
        transferItem.setDatasetVersion(fileContentAttributes.getDatasetVersion());
        transferItem.setBagId(fileContentAttributes.getBagId());
        transferItem.setNbn(fileContentAttributes.getNbn());
        transferItem.setOaiOre(fileContentAttributes.getOaiOre());
        transferItem.setPidMapping(fileContentAttributes.getPidMapping());

        return transferItemDAO.save(transferItem);
    }
}
