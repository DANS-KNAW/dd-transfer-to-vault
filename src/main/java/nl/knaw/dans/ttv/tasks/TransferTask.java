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
package nl.knaw.dans.ttv.tasks;

import io.dropwizard.servlets.tasks.Task;
import nl.knaw.dans.ttv.core.Inbox;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import nl.knaw.dans.ttv.exceptions.InvalidTransferItemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipFile;

public class TransferTask extends Task implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TransferTask.class);
    private static final String expectedFilePattern = "" + "(?<doi>doi-10-[0-9]{4,}-[A-Za-z0-9]{2,}-[A-Za-z0-9]{6})-?" + "(?<schema>datacite)?.?" + "v(?<major>[0-9]+).(?<minor>[0-9]+)" + "(?<extension>.zip|.xml)";
    private static final Pattern pattern = Pattern.compile(expectedFilePattern);
    private final List<Inbox> inboxes;
    private final TransferItemDAO transferItemDAO;


    public TransferTask(List<Inbox> inboxes, TransferItemDAO transferItemDAO) {
        super(TransferTask.class.getName());
        this.inboxes = inboxes;
        this.transferItemDAO = transferItemDAO;
    }

    @Override
    public void run() {
        log.info("Running task" + this);
        try {
            buildTransferQueue();
        } catch (IOException e) {
            throw new InvalidTransferItemException("Invalid TransferItem",e);
        }

    }

    @Override
    public void execute(Map<String, List<String>> map, PrintWriter printWriter){
        run();
    }

    public void buildTransferQueue() throws IOException {
        //walk inbox and filter .zip files
        //match file name to pattern and group pid, version, creationTime and metadata file path -> create TransferItem
        walkInboxPathsAndFilterDve(inboxes).forEach((datasetVersionExportPath) -> transferItemDAO.create(dissectDve(datasetVersionExportPath)));

        //TODO for items already in the database, verify that they are consistent with what is found on disk.

    }

    public List<Path> walkInboxPathsAndFilterDve(List<Inbox> inboxes) throws IOException {
        List<Path> inboxPaths = inboxes.stream().map(Inbox::getPath).collect(Collectors.toList());
        List<Path> datasetVersionExports = new java.util.ArrayList<>(Collections.emptyList());
        for (Path path : inboxPaths) {
            datasetVersionExports
                    .addAll(Files.walk(path, 1)
                            .filter(Files::isRegularFile)
                            .filter(path1 -> path1.getFileName().toString().endsWith(".zip"))
                            .collect(Collectors.toList()));
        }
        return datasetVersionExports;
    }

    public TransferItem dissectDve(Path datasetVersionExportPath){
        Matcher matcher = pattern.matcher(datasetVersionExportPath.getFileName().toString());

        TransferItem transferItem = new TransferItem();

        if (matcher.matches()) {
            if (matcher.group("doi") != null) {
                String datasetPid = matcher.group("doi").substring(4).toUpperCase().replaceFirst("-", ".").replaceAll("-", "/");
                transferItem.setDatasetPid(datasetPid);
            }
            if (matcher.group("major") != null) {
                transferItem.setVersionMajor(Integer.parseInt(matcher.group("major")));
            }
            if (matcher.group("minor") != null) {
                transferItem.setVersionMinor(Integer.parseInt(matcher.group("minor")));
            }
            try {
                if (Files.getAttribute(datasetVersionExportPath, "creationTime") != null){
                    FileTime creationTime = (FileTime) Files.getAttribute(datasetVersionExportPath, "creationTime");
                    transferItem.setCreationTime(LocalDateTime.ofInstant(creationTime.toInstant(), ZoneId.systemDefault()));
                }
            } catch (IOException e) {
                throw new InvalidTransferItemException("Invalid TransferItem",e);
            }
            String metadataFilePath;
            try {
                metadataFilePath = Objects.requireNonNull(new ZipFile(datasetVersionExportPath.toFile()).stream().filter(zipEntry -> zipEntry.getName().endsWith(".jsonld")).findAny().orElse(null)).getName();
            } catch (IOException e) {
                throw new InvalidTransferItemException("Invalid TransferItem",e);
            }
            transferItem.setMetadataFile(metadataFilePath);
            transferItem.setTransferStatus(TransferItem.TransferStatus.EXTRACT);
        }
        return transferItem;

    }

    public void extractMetadata() {
        //TODO pre condition: transfer-inbox contains Dataset-Version-Exports (DVE) as exported from the Data Station, without the additional files
        //

        /* TODO Extract PID and version from DVE name
         * TODO Using PID and version, obtain dataset metadata, including Data Vault extension metadata, from DVE oai-ore.jsonld. This yields: DV PID, DV PID version, bagID, NBN, Other ID, Other ID version, SWORD token from Data Vault extension metadata
         * TODO Obtain Source DV instance from directory in which DVE resides
         * TODO Compute hash over zipped DVE to use as BagChecksum
         * TODO Update TransferItem with all the extracted metadata. Update the status to MOVE
         *  */

        //TODO post condition: TransferItem created, xml-file deleted
    }

}
