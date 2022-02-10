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

import nl.knaw.dans.ttv.core.InvalidTransferItemException;
import nl.knaw.dans.ttv.core.dto.ArchiveMetadata;
import nl.knaw.dans.ttv.core.dto.FileContentAttributes;
import nl.knaw.dans.ttv.core.dto.FilenameAttributes;
import nl.knaw.dans.ttv.core.dto.FilesystemAttributes;
import nl.knaw.dans.ttv.db.Tar;
import nl.knaw.dans.ttv.db.TransferItem;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface TransferItemService {

    TransferItem createTransferItem(String datastationName, FilenameAttributes filenameAttributes, FilesystemAttributes filesystemAttributes, FileContentAttributes fileContentAttributes)
        throws InvalidTransferItemException;

    TransferItem createTransferItem(String datastationName, FilenameAttributes filenameAttributes, FilesystemAttributes filesystemAttributes)
        throws InvalidTransferItemException;

    TransferItem moveTransferItem(TransferItem transferItem, TransferItem.TransferStatus newStatus, Path newPath);

    void saveAllTars(List<Tar> tars);

    void updateTarToCreated(String id, ArchiveMetadata metadata);

    List<Tar> stageAllTarsToBeConfirmed();

    void updateConfirmArchivedResult(Tar tar, Tar.TarStatus status);

    Optional<TransferItem> getTransferItemByFilenameAttributes(FilenameAttributes filenameAttributes);

    TransferItem addMetadataAndMoveFile(TransferItem transferItem, FileContentAttributes fileContentAttributes, TransferItem.TransferStatus status, Path newPath);

    Tar createTarArchiveWithAllCollectedTransferItems(UUID uuid, String vaultPath);

    Tar save(Tar tarArchive);

    void resetTarToArchiving(Tar tar);

    void updateTarToArchived(Tar tar);
}
