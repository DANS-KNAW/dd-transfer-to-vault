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
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ttv.core.InvalidTransferItemException;
import nl.knaw.dans.ttv.core.domain.ArchiveMetadata;
import nl.knaw.dans.ttv.core.domain.FileContentAttributes;
import nl.knaw.dans.ttv.core.domain.FilenameAttributes;
import nl.knaw.dans.ttv.core.domain.FilesystemAttributes;
import nl.knaw.dans.ttv.core.Tar;
import nl.knaw.dans.ttv.db.TarDao;
import nl.knaw.dans.ttv.core.TarPart;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.ttv.db.TransferItemDao;
import org.hibernate.Hibernate;
import org.hibernate.ObjectNotFoundException;

import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class TransferItemServiceImpl implements TransferItemService {
    private final TransferItemDao transferItemDAO;
    private final TarDao tarDAO;

    public TransferItemServiceImpl(TransferItemDao transferItemDAO, TarDao tarDAO) {
        this.transferItemDAO = transferItemDAO;
        this.tarDAO = tarDAO;
    }

    @Override
    @UnitOfWork
    public TransferItem createTransferItem(
        String datastationName,
        FilenameAttributes filenameAttributes,
        FilesystemAttributes filesystemAttributes,
        FileContentAttributes fileContentAttributes
    ) throws InvalidTransferItemException {
        var transferItem = new TransferItem();

        transferItem.setTransferStatus(TransferItem.TransferStatus.COLLECTED);
        transferItem.setDatastation(datastationName);
        transferItem.setQueueTimestamp(OffsetDateTime.now());

        // filename attributes
        transferItem.setDveFilePath(filenameAttributes.getDveFilePath());
        transferItem.setDveFilename(filenameAttributes.getDveFilename());

        // filesystem attributes
        transferItem.setCreationTime(filesystemAttributes.getCreationTime());
        transferItem.setBagSize(filesystemAttributes.getBagSize());
        transferItem.setBagSha256Checksum(filesystemAttributes.getChecksum());

        // file content attributes
        if (fileContentAttributes != null) {
            transferItem.setDataversePidVersion(fileContentAttributes.getDataversePidVersion());
            transferItem.setBagId(fileContentAttributes.getBagId());
            transferItem.setNbn(fileContentAttributes.getNbn());
            transferItem.setMetadata(fileContentAttributes.getMetadata());
            transferItem.setFilepidToLocalPath(fileContentAttributes.getFilepidToLocalPath());
        }

        // check if an item with this ID already exists
        var existing = transferItemDAO.findByIdentifier(transferItem.getDveFilename())
            .filter(item -> Objects.equals(item.getBagSha256Checksum(), transferItem.getBagSha256Checksum()));

        if (existing.isPresent()) {
            throw new InvalidTransferItemException(
                String.format("TransferItem with dveFilename=%s and identical checksums already exists in database",
                    transferItem.getDveFilename()
                )
            );
        }

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

            tarDAO.save(tar);
            return tar;
        });
    }

    @Override
    @UnitOfWork
    public Optional<Tar> updateTarToCreated(String id, ArchiveMetadata metadata) {
        return tarDAO.findById(id).map(tar -> {
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

            tarDAO.saveWithParts(tar, parts);
            return tar;
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
        if (filenameAttributes == null) {
            return Optional.empty();
        }

        if (filenameAttributes.getInternalId() != null) {
            return transferItemDAO.findById(filenameAttributes.getInternalId());
        }

        return transferItemDAO.findByIdentifier(filenameAttributes.getDveFilename());
    }

    @Override
    public TransferItem addMetadata(TransferItem transferItem, FileContentAttributes fileContentAttributes) {

        Objects.requireNonNull(transferItem, "transferItem cannot be null");
        Objects.requireNonNull(fileContentAttributes, "fileContentAttributes cannot be null");

        // file content attributes
        transferItem.setDataversePid(fileContentAttributes.getDataversePid());
        transferItem.setDataversePidVersion(fileContentAttributes.getDataversePidVersion());
        transferItem.setBagId(fileContentAttributes.getBagId());
        transferItem.setNbn(fileContentAttributes.getNbn());
        transferItem.setMetadata(fileContentAttributes.getMetadata());
        transferItem.setFilepidToLocalPath(fileContentAttributes.getFilepidToLocalPath());
        transferItem.setOtherId(fileContentAttributes.getOtherId());
        transferItem.setOtherIdVersion(fileContentAttributes.getOtherIdVersion());
        transferItem.setSwordToken(fileContentAttributes.getSwordToken());
        transferItem.setDataSupplier(fileContentAttributes.getDataSupplier());

        return transferItem;
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
        tarArchive.setCreated(OffsetDateTime.now());
        tarArchive.setVaultPath(vaultPath);
        tarArchive.setArchiveInProgress(true);

        var transferItems = transferItemDAO.findByStatus(TransferItem.TransferStatus.METADATA_EXTRACTED);

        for (var transferItem : transferItems) {
            transferItem.setTransferStatus(TransferItem.TransferStatus.TARRING);
            transferItem.setTar(tarArchive);

            transferItemDAO.merge(transferItem);
        }

        tarArchive.setTransferItems(transferItems);

        return tarDAO.save(tarArchive);
    }

    @Override
    @UnitOfWork
    public Tar save(Tar tarArchive) {
        for (var transferItem: tarArchive.getTransferItems()) {
            transferItemDAO.merge(transferItem);
        }

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
        tar.setArchivalTimestamp(OffsetDateTime.now());
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
