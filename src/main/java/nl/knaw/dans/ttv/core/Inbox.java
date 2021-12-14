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

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.apache.commons.codec.digest.DigestUtils;
import io.dropwizard.hibernate.UnitOfWork;
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Inbox {

    private static final Logger log = LoggerFactory.getLogger(Inbox.class);

    private final String name;
    private final Path path;

    private TransferItemDAO transferItemDAO;
    private SessionFactory sessionFactory;
    private ObjectMapper objectMapper;

    private static final String DOI_PATTERN = "(?<doi>doi-10-[0-9]{4,}-[A-Za-z0-9]{2,}-[A-Za-z0-9]{6})-?";
    private static final String SCHEMA_PATTERN = "(?<schema>datacite)?.?";
    private static final String DATASET_VERSION_PATTERN = "v(?<major>[0-9]+).(?<minor>[0-9]+)";
    private static final String EXTENSION_PATTERN = "(?<extension>.zip|.xml)";
    private static final Pattern PATTERN = Pattern.compile(DOI_PATTERN + SCHEMA_PATTERN + DATASET_VERSION_PATTERN + EXTENSION_PATTERN);
    public static final Comparator<Task> TASK_QUEUE_DATE_COMPARATOR = Comparator.comparing(task -> task.getTransferItem().getCreationTime());

    public Inbox(String name, Path path) {
        this.name = name;
        this.path = path;
    }

    @UnitOfWork
    public List<Task> createTransferItemTasks() {
        List<Task> transferItemTasks = new java.util.ArrayList<>(Collections.emptyList());
        List<TransferItem> transferItemsOnDisk = createTransferItemsFromDisk();

        transferItemsOnDisk.forEach(transferItemDAO::save);
        List<TransferItem> transferItemsInDB = transferItemDAO.findAllStatusExtract();

        // consistency check
        if (!transferItemsOnDisk.containsAll(transferItemsInDB)) {
            log.error("Inconsistency found with TransferItems found on disk and in database");
            throw new InvalidTransferItemException("Inconsistency found with TransferItems found on disk and in database");
        } else {
            for (TransferItem transferItem : transferItemsInDB) {
                TransferTask transferTask = new UnitOfWorkAwareProxyFactory("UnitOfWorkProxy", sessionFactory)
                        .create(TransferTask.class, new Class[] {TransferItem.class, TransferItemDAO.class, ObjectMapper.class}, new Object[] {transferItem, transferItemDAO, objectMapper});
                transferItemTasks.add(transferTask);
            }
        }
        return transferItemTasks;
    }

    @UnitOfWork
    public Task createTransferItemTask(Path datasetVersionExportPath) {
        TransferItem transferItem = transferItemDAO.save(transformDvePathToTransferItem(datasetVersionExportPath));
        return new UnitOfWorkAwareProxyFactory("TransferTaskProxy", sessionFactory)
                .create(TransferTask.class, new Class[] {TransferItem.class, TransferItemDAO.class, ObjectMapper.class}, new Object[] {transferItem, transferItemDAO, objectMapper});
    }

    private List<TransferItem> createTransferItemsFromDisk() {
        List<TransferItem> transferItemsOnDisk;
        try {
            transferItemsOnDisk = Files.walk(path, 1)
                    .filter(Files::isRegularFile)
                    .filter(path1 -> path1.getFileName().toString().endsWith(".zip"))
                    .map(this::transformDvePathToTransferItem).collect(Collectors.toList());
        } catch (IOException ioException) {
            log.error("Invalid TransferItem");
            throw new InvalidTransferItemException("Invalid TransferItem", ioException);
        }
        return transferItemsOnDisk;
    }

    public TransferItem transformDvePathToTransferItem(Path datasetVersionExportPath){
        Matcher matcher = PATTERN.matcher(datasetVersionExportPath.getFileName().toString());

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
                    transferItem.setBagChecksum(new DigestUtils("SHA-256").digestAsHex(Files.readAllBytes(datasetVersionExportPath)));
                    transferItem.setBagSize(Files.size(datasetVersionExportPath));
                }
            } catch (IOException e) {
                throw new InvalidTransferItemException("Invalid TransferItem",e);
            }
            transferItem.setDveFilePath(String.valueOf(datasetVersionExportPath));
            transferItem.setTransferStatus(TransferItem.TransferStatus.EXTRACT);
            transferItem.setDatasetDvInstance(name);
        }
        return transferItem;
    }

    public void setTransferItemDAO(TransferItemDAO transferItemDAO) {
        this.transferItemDAO = transferItemDAO;
    }

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }
    
    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public Path getPath() {
        return path;
    }

    

    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String toString() {
        return "Inbox{" +
                "name='" + name + '\'' +
                ", path=" + path +
                '}';
    }
}
