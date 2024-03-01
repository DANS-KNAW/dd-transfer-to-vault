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
import nl.knaw.dans.ttv.core.domain.ArchiveMetadata;
import nl.knaw.dans.ttv.core.domain.FileContentAttributes;
import nl.knaw.dans.ttv.core.domain.FilenameAttributes;
import nl.knaw.dans.ttv.core.domain.FilesystemAttributes;
import nl.knaw.dans.ttv.core.Tar;
import nl.knaw.dans.ttv.core.TransferItem;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public interface TransferItemService {

    TransferItem createTransferItem(String datastationName, FilenameAttributes filenameAttributes, FilesystemAttributes filesystemAttributes, FileContentAttributes fileContentAttributes)
        throws InvalidTransferItemException;

    TransferItem moveTransferItem(TransferItem transferItem, TransferItem.TransferStatus newStatus, Path newPath) throws IOException;

    void saveAllTars(List<Tar> tars);

    void setArchiveAttemptFailed(String id, boolean increaseAttemptCount, int maxRetries);

    Optional<Tar> updateTarToCreated(String id, ArchiveMetadata metadata);

    void setArchivingInProgress(String id);

    List<Tar> stageAllTarsToBeConfirmed();

    Optional<TransferItem> getTransferItemByFilenameAttributes(FilenameAttributes filenameAttributes);

    TransferItem addMetadata(TransferItem transferItem, FileContentAttributes fileContentAttributes);

    Optional<Tar> getTarById(String id);

    Tar createTarArchiveWithAllMetadataExtractedTransferItems(String id, String vaultPath);

    Tar save(Tar tarArchive);

    void resetTarToArchiving(Tar tar);

    void updateTarToArchived(Tar tar);

    List<Tar> findTarsByStatusTarring();

    List<Tar> findTarsByConfirmInProgress();

    List<Tar> findTarsToBeRetried();

    public Optional<TransferItem> findByDveFilename(String name);

}
