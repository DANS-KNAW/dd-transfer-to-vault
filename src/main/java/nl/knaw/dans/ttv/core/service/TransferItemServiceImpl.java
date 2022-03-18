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
import nl.knaw.dans.ttv.core.dto.ArchiveMetadata;
import nl.knaw.dans.ttv.core.dto.FileContentAttributes;
import nl.knaw.dans.ttv.core.dto.FilenameAttributes;
import nl.knaw.dans.ttv.core.dto.FilesystemAttributes;
import nl.knaw.dans.ttv.db.Tar;
import nl.knaw.dans.ttv.db.TarDAO;
import nl.knaw.dans.ttv.db.TarPart;
import nl.knaw.dans.ttv.db.TransferItem;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.hibernate.Hibernate;
import org.hibernate.ObjectNotFoundException;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class TransferItemServiceImpl implements TransferItemService {
    private final TransferItemDAO transferItemDAO;
    private final TarDAO tarDAO;

    public TransferItemServiceImpl(TransferItemDAO transferItemDAO, TarDAO tarDAO) {
        this.transferItemDAO = transferItemDAO;
        this.tarDAO = tarDAO;
    }

    @Override
    @UnitOfWork
    public TransferItem createTransferItem(String datastationName, FilenameAttributes filenameAttributes, FilesystemAttributes filesystemAttributes,
        FileContentAttributes fileContentAttributes)
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
        transferItem.setBagSize(filesystemAttributes.getBagSize());

        // file content attributes
        transferItem.setDatasetVersion(fileContentAttributes.getDatasetVersion());
        transferItem.setBagId(fileContentAttributes.getBagId());
        transferItem.setNbn(fileContentAttributes.getNbn());
        transferItem.setOaiOre(fileContentAttributes.getOaiOre());
        transferItem.setPidMapping(fileContentAttributes.getPidMapping());
        transferItem.setBagChecksum(fileContentAttributes.getBagChecksum());

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
    public TransferItem createTransferItem(String datastationName, FilenameAttributes filenameAttributes, FilesystemAttributes filesystemAttributes)
        throws InvalidTransferItemException {

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

        transferItem.setTransferStatus(TransferItem.TransferStatus.COLLECTED);
        transferItem.setQueueDate(LocalDateTime.now());
        transferItem.setDatasetDvInstance(datastationName);
        transferItem.setBagDepositDate(LocalDateTime.now());

        // filename attributes
        transferItem.setDveFilePath(filenameAttributes.getDveFilePath());
        transferItem.setDatasetPid(filenameAttributes.getDatasetPid());
        transferItem.setVersionMajor(filenameAttributes.getVersionMajor());
        transferItem.setVersionMinor(filenameAttributes.getVersionMinor());

        // filesystem attributes
        transferItem.setCreationTime(filesystemAttributes.getCreationTime());
        transferItem.setBagSize(filesystemAttributes.getBagSize());

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

    @UnitOfWork
    public void saveAllTransferItems(List<TransferItem> transferItems) {
        for (var transferItem : transferItems) {
            transferItemDAO.merge(transferItem);
        }
    }

    @Override
    @UnitOfWork
    public void saveAllTars(List<Tar> tars) {
        for (var tar : tars) {
            tarDAO.save(tar);
        }
    }

    @Override
    @UnitOfWork
    public void setArchiveAttemptFailed(String id, boolean increaseAttemptCount, int maxRetries) {
        tarDAO.findById(id).map(tar -> {
            tar.setTarStatus(Tar.TarStatus.TARRING);
            tar.setArchiveInProgress(false);

            if (increaseAttemptCount) {
                tar.setTransferAttempt(tar.getTransferAttempt() + 1);
            }

            // for example, if maxRetries = 2 and the task just failed for the first time,
            // the transferAttempt would be set to 1. We still want to attempt it 2 more times
            // hence the > and not >=
            if (tar.getTransferAttempt() > maxRetries) {
                tar.setTarStatus(Tar.TarStatus.OCFLTARFAILED);
            }

            return tarDAO.save(tar);
        });
    }

    @Override
    @UnitOfWork
    public void updateTarToCreated(String id, ArchiveMetadata metadata) {
        tarDAO.findById(id).map(tar -> {
            tar.setTarStatus(Tar.TarStatus.OCFLTARCREATED);
            tar.setArchiveInProgress(false);

            for (var transferItem : tar.getTransferItems()) {
                transferItem.setTransferStatus(TransferItem.TransferStatus.OCFLTARCREATED);
            }

            var parts = metadata.getParts().stream().map(part -> {
                var dbPart = new TarPart();
                dbPart.setTar(tar);
                dbPart.setPartName(part.getIdentifier());
                dbPart.setChecksumValue(part.getChecksum());
                dbPart.setChecksumAlgorithm(part.getChecksumAlgorithm());

                return dbPart;
            }).collect(Collectors.toList());

            return tarDAO.saveWithParts(tar, parts);
        });
    }

    @Override
    @UnitOfWork
    public void setArchivingInProgress(String id) {
        tarDAO.findById(id).
            map(tar -> {
                tar.setTarStatus(Tar.TarStatus.TARRING);
                tar.setArchiveInProgress(true);

                return tarDAO.save(tar);
            }).orElseThrow(() -> new ObjectNotFoundException(id, "Tar"));
    }

    @Override
    @UnitOfWork
    public List<Tar> stageAllTarsToBeConfirmed() {
        var results = tarDAO.findAllTarsToBeConfirmed();

        for (var item : results) {
            item.setConfirmCheckInProgress(true);
        }

        saveAllTars(results);

        return results;
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
    public TransferItem addMetadataAndMoveFile(TransferItem transferItem, FileContentAttributes fileContentAttributes, TransferItem.TransferStatus status,
        Path newPath) {

        Objects.requireNonNull(transferItem, "transferItem cannot be null");
        Objects.requireNonNull(fileContentAttributes, "fileContentAttributes cannot be null");
        Objects.requireNonNull(status, "status cannot be null");
        Objects.requireNonNull(newPath, "newPath cannot be null");

        transferItem.setTransferStatus(status);
        transferItem.setDveFilePath(newPath.toString());

        // file content attributes
        transferItem.setDatasetVersion(fileContentAttributes.getDatasetVersion());
        transferItem.setBagId(fileContentAttributes.getBagId());
        transferItem.setNbn(fileContentAttributes.getNbn());
        transferItem.setOaiOre(fileContentAttributes.getOaiOre());
        transferItem.setPidMapping(fileContentAttributes.getPidMapping());
        transferItem.setBagChecksum(fileContentAttributes.getBagChecksum());
        transferItem.setOtherId(fileContentAttributes.getOtherId());
        transferItem.setOtherIdVersion(fileContentAttributes.getOtherIdVersion());
        transferItem.setSwordToken(fileContentAttributes.getSwordToken());
        transferItem.setSwordClient(fileContentAttributes.getSwordClient());

        return transferItemDAO.save(transferItem);
    }

    @Override
    @UnitOfWork
    public Optional<Tar> getTarById(String id) {
        return tarDAO.findById(id).map(tar -> {
            Hibernate.initialize(tar.getTarParts());
            Hibernate.initialize(tar.getTransferItems());

            return tar;
        });
    }

    @Override
    @UnitOfWork
    public Tar createTarArchiveWithAllMetadataExtractedTransferItems(String id, String vaultPath) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(vaultPath, "vaultPath cannot be null");

        var tarArchive = new Tar();
        tarArchive.setTarUuid(id);
        tarArchive.setTarStatus(Tar.TarStatus.TARRING);
        tarArchive.setCreated(LocalDateTime.now());
        tarArchive.setVaultPath(vaultPath);
        tarArchive.setArchiveInProgress(true);

        var transferItems = transferItemDAO.findByStatus(TransferItem.TransferStatus.METADATA_EXTRACTED);

        for (var transferItem : transferItems) {
            transferItem.setTransferStatus(TransferItem.TransferStatus.TARRING);
            transferItem.setAipsTar(tarArchive);
        }

        tarArchive.setTransferItems(transferItems);

        return tarDAO.save(tarArchive);
    }

    @Override
    @UnitOfWork
    public Tar save(Tar tarArchive) {
        return tarDAO.save(tarArchive);
    }

    @Override
    public void resetTarToArchiving(Tar tar) {
        tar.setTarStatus(Tar.TarStatus.OCFLTARCREATED);
        tar.setConfirmCheckInProgress(false);
        save(tar);
    }

    @Override
    public void updateTarToArchived(Tar tar) {
        tar.setTarStatus(Tar.TarStatus.CONFIRMEDARCHIVED);
        tar.setConfirmCheckInProgress(false);
        tar.setDatetimeConfirmedArchived(LocalDateTime.now());
        save(tar);
    }

    @Override
    @UnitOfWork
    public List<Tar> findTarsByStatusTarring() {
        var tars = tarDAO.findByStatus(Tar.TarStatus.TARRING);

        for (var tar : tars) {
            Hibernate.initialize(tar.getTransferItems());
        }

        return tars;
    }

    @Override
    @UnitOfWork
    public List<Tar> findTarsByConfirmInProgress() {
        return tarDAO.findTarsByConfirmInProgress();
    }

    @Override
    @UnitOfWork
    public List<Tar> findTarsToBeRetried() {
        return tarDAO.findTarsToBeRetried();
    }
}
