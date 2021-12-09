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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import io.dropwizard.hibernate.UnitOfWork;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.zip.ZipFile;

public class TransferTask extends Task {

    private static final Logger log = LoggerFactory.getLogger(TransferTask.class);

    public TransferTask(TransferItem transferItem, TransferItemDAO transferItemDAO, OcflRepository ocflRepository, ObjectMapper objectMapper) {
        super(transferItem, transferItemDAO, ocflRepository, objectMapper);
    }

    @Override
    public void run() {
        log.info("Running task: " + this);
        try {
            extractMetadata();
            generateOcflArchiveAndStageForTransfer();
        } catch (NoSuchElementException | IOException | NullPointerException e) {
            log.error(e.getMessage(), e);
            throw new InvalidTransferItemException(e.getMessage());
        }
    }

    @UnitOfWork
    public void extractMetadata() throws IOException {
        ZipFile datasetVersionExport = new ZipFile(Paths.get(transferItem.getDveFilePath()).toFile());
        String metadataFilePath = Objects.requireNonNull(datasetVersionExport.stream()
                .filter(zipEntry -> zipEntry.getName().endsWith(".jsonld"))
                .findAny().orElse(null), "metadataFilePath can't be null" + transferItem.onError()).getName();
        String pidMappingFilePath = Objects.requireNonNull(datasetVersionExport.stream()
                .filter(zipEntry -> zipEntry.getName().contains("pid-mapping.txt"))
                .findAny().orElse(null), "pidMappingFilePath can't be null" + transferItem.onError()).getName();
        byte[] pidMapping = Objects.requireNonNull(IOUtils.toByteArray(datasetVersionExport.getInputStream(datasetVersionExport.getEntry(pidMappingFilePath))), "pidMapping can't be null" + transferItem.onError());
        byte[] oaiOre = Objects.requireNonNull(IOUtils.toByteArray(datasetVersionExport.getInputStream(datasetVersionExport.getEntry(metadataFilePath))), "oaiOre can't be null" + transferItem.onError());

        JsonNode jsonNode = Objects.requireNonNull(objectMapper.readTree(oaiOre), "jsonld metadata can't be null" + transferItem.onError());

        String nbn = Objects.requireNonNull(jsonNode.get("ore:describes").get("dansDataVaultMetadata:NBN").asText(), "NBN can't be null" + transferItem.onError());
        String dvPidVersion = Objects.requireNonNull(jsonNode.get("ore:describes").get("dansDataVaultMetadata:DV PID Version").asText(), "DV PID Version can't be null" + transferItem.onError());
        String bagId = Objects.requireNonNull(jsonNode.get("ore:describes").get("dansDataVaultMetadata:Bag ID").asText(), "Bag ID can't be null" + transferItem.onError());
        //TODO Other ID, Other ID Version and SWORD token missing
        transferItem.setPidMapping(pidMapping);
        transferItem.setOaiOre(oaiOre);
        transferItem.setNbn(nbn);
        transferItem.setDatasetVersion(dvPidVersion);
        transferItem.setBagId(bagId);
        transferItem.setQueueDate(LocalDateTime.now());
        transferItem.setTransferStatus(TransferItem.TransferStatus.MOVE);
        transferItemDAO.save(transferItem);
    }

    @UnitOfWork
    public void generateOcflArchiveAndStageForTransfer() throws IOException {
        if (transferItem.equals(transferItemDAO.findById(transferItem.getId()).orElseThrow())) {
            String bagId = Objects.requireNonNull(transferItem.getBagId(), "Bag ID can't be null" + transferItem.onError());
            String objectId = bagId.substring(0,9) + bagId.substring(9, 11) + "/" + bagId.substring(11,13) + "/" + bagId.substring(13,15) + "/" + bagId.substring(15);
            Path source = Objects.requireNonNull(Path.of(transferItem.getDveFilePath()), "dveFilePath can't be null" + transferItem.onError());
            Path target = Objects.requireNonNull(Paths.get(transferItem.getDveFilePath()).getParent().resolve(Paths.get(bagId + ".zip")), "dveFilePath can't be null" + transferItem.onError());

            //TODO change to Files.move(source, target) when tested.
            Files.copy(source, target);
            ocflRepository.putObject(ObjectVersionId.head(objectId), target, new VersionInfo().setMessage("initial commit"), OcflOption.MOVE_SOURCE);

            //update transferItem
            String aipTarEntryName = Inbox.TRANSFER_OUTBOX + "/" + objectId.substring(9);
            if (Files.exists(Paths.get(aipTarEntryName))) {
                transferItem.setTransferStatus(TransferItem.TransferStatus.TAR);
                transferItem.setAipTarEntryName(aipTarEntryName);
                transferItemDAO.merge(transferItem);
                transferItemDAO.flush();
            } else {
                log.error(aipTarEntryName + " doesn't exist");
                throw new InvalidTransferItemException(aipTarEntryName + " doesn't exist");
            }
        }
    }

    @Override
    public String toString() {
        return "TransferTask{" +
                "transferItem=" + transferItem.getDveFilePath() +
                '}';
    }
}
