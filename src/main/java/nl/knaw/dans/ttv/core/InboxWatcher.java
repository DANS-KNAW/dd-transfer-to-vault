package nl.knaw.dans.ttv.core;

import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipFile;

public class InboxWatcher {

    private static final Logger log = LoggerFactory.getLogger(InboxWatcher.class);

    private static final String DOI_PATTERN = "(?<doi>doi-10-[0-9]{4,}-[A-Za-z0-9]{2,}-[A-Za-z0-9]{6})-?";
    private static final String SCHEMA_PATTERN = "(?<schema>datacite)?.?";
    private static final String DATASET_VERSION_PATTERN = "v(?<major>[0-9]+).(?<minor>[0-9]+)";
    private static final String EXTENSION_PATTERN = "(?<extension>.zip|.xml)";
    private static final Pattern PATTERN = Pattern.compile(DOI_PATTERN + SCHEMA_PATTERN + DATASET_VERSION_PATTERN + EXTENSION_PATTERN);

    private List<Inbox> inboxList;
    private TransferItemDAO transferItemDAO;
    private ExecutorService queueProcessorService;

    public InboxWatcher(List<Inbox> inboxList, TransferItemDAO transferItemDAO, ExecutorService queueProcessorService) {
        this.inboxList = inboxList;
        this.transferItemDAO = transferItemDAO;
        this.queueProcessorService = queueProcessorService;
    }

    public List<Path> walkTransferInboxPathsAndFilterDve(List<Inbox> inboxes) throws IOException {
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

    public List<Task<TransferItem>> createTask(TransferItem transferItem){

        return null;
    }

}
