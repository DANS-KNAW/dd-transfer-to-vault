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
import io.dropwizard.hibernate.UnitOfWork;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.zip.ZipFile;

public class TransferTask extends Task {

    private static final Logger log = LoggerFactory.getLogger(TransferTask.class);

    public TransferTask(TransferItem transferItem, TransferItemDAO transferItemDAO, ObjectMapper objectMapper) {
        super(transferItem, transferItemDAO, objectMapper);
    }

    @Override
    public void run() {
        log.info("Running task: " + this);
        try {
            extractMetadata();
        } catch (IOException | NullPointerException e) {
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

        String nbn = Objects.requireNonNull(jsonNode.get("ore:describes").get("dansDataVaultMetadata:NBN").toString(), "NBN can't be null" + transferItem.onError());
        String dvPidVersion = Objects.requireNonNull(jsonNode.get("ore:describes").get("dansDataVaultMetadata:DV PID Version").toString(), "DV PID Version can't be null" + transferItem.onError());
        String bagId = Objects.requireNonNull(jsonNode.get("ore:describes").get("dansDataVaultMetadata:Bag ID").toString(), "Bag ID can't be null" + transferItem.onError());
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

    public void generateOCFLarchiveAndStageForTransfer() {
        //TODO
    }

    @Override
    public String toString() {
        return "TransferTask{" +
                "transferItem=" + transferItem.getDveFilePath() +
                '}';
    }
}
