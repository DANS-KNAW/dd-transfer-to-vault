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

import lombok.NonNull;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.zip.ZipFile;

public class FileServiceImpl implements FileService {
    private static final Logger log = LoggerFactory.getLogger(FileServiceImpl.class);

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @Override
    public Path moveFile(@NonNull Path current, @NonNull Path newPath) throws IOException {
        log.debug("moving file from {} to {}", current, newPath);

        if (Files.exists(newPath)) {
            throw new FileAlreadyExistsException(String.format("Cannot move file %s to %s, file already exists", current, newPath));
        }

        return Files.move(current, newPath);
    }

    @Override
    public Path moveFileAtomically(@NonNull Path filePath, @NonNull Path newPath) throws IOException {
        var tempTarget = newPath.getParent().resolve(newPath.getFileName() + ".part");

        var store1 = Files.getFileStore(filePath);
        var store2 = Files.getFileStore(newPath.getParent());

        if (store1.equals(store2)) {
            log.debug("Moving file {} to {}", filePath, newPath);
            return moveFile(filePath, newPath);
        }

        // there could be leftovers from a previous attempt, remove them
        Files.deleteIfExists(tempTarget);

        log.debug("Moving file atomically {} to {}", filePath, newPath);
        moveFile(filePath, tempTarget);
        return moveFile(tempTarget, newPath);
    }

    @Override
    public void ensureDirectoryExists(@NonNull Path path) throws IOException {
        Files.createDirectories(path);
    }

    /**
     * Moves a file to the rejected/ subfolder of where it was initially found If a file already exists, it will append a number to the filename.
     * <p>
     * It also writes an error description inside a file which is named `filename`.error.txt (or `filename`.1.error.txt` if it already exists)
     *
     * @param path The path to the file to reject
     */
    @Override
    public void rejectFile(@NonNull Path path, @NonNull Throwable exception) throws IOException {
        var rejectedFolder = path.getParent().resolve("rejected");
        ensureDirectoryExists(rejectedFolder);

        var filename = path.getFileName().toString();

        log.debug("Moving rejected file to '{}'", rejectedFolder);
        var extension = FilenameUtils.getExtension(filename);
        var fileBaseName = FilenameUtils.removeExtension(filename);

        log.debug("File base name is '{}', extension is '{}'", fileBaseName, extension);
        var rejectedFileName = fileBaseName + "." + extension;
        var errorFileName = fileBaseName + ".error.txt";

        var duplicateCounter = 1;

        while (Files.exists(rejectedFolder.resolve(rejectedFileName))) {
            log.debug("File '{}' already exists, generating a new filename", rejectedFolder.resolve(rejectedFileName));
            rejectedFileName = fileBaseName + "_" + duplicateCounter + "." + extension;
            errorFileName = fileBaseName + "_" + duplicateCounter + ".error.txt";

            duplicateCounter += 1;
        }

        log.debug("Settled on '{}' for filename", rejectedFileName);
        var targetPath = rejectedFolder.resolve(rejectedFileName);
        var targetErrorPath = rejectedFolder.resolve(errorFileName);

        log.debug("Moving file to '{}', writing error report to '{}'", targetPath, targetErrorPath);

        Files.move(path, targetPath);
        writeExceptionToFile(targetErrorPath, exception);
    }

    @Override
    public boolean exists(Path path) {
        return Files.exists(path);
    }

    @Override
    public boolean canRead(Path path) {
        return Files.isReadable(path);
    }

    @Override
    public boolean canRead(Path path, int timeout) throws TimeoutException {
        var future = executorService.submit(() -> Files.isReadable(path));
        try {
            return future.get(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            future.cancel(true);
        }
    }

    @Override
    public boolean canWrite(Path path) {
        return Files.isWritable(path);
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
        return Files.getFileStore(path);
    }

    void writeExceptionToFile(@NonNull Path errorReportName, @NonNull Throwable exception) throws IOException {
        var writer = new StringWriter();
        var pw = new PrintWriter(writer);
        exception.printStackTrace(pw);
        var stackTrace = writer.toString();
        pw.close();

        Files.write(errorReportName, stackTrace.getBytes());
    }

    @Override
    public Object getFilesystemAttribute(@NonNull Path path, @NonNull String property) throws IOException {
        log.debug("Getting attribute {} for path '{}'", property, path);
        return Files.getAttribute(path, property);
    }

    @Override
    public String calculateChecksum(@NonNull Path path) throws IOException {
        log.debug("Calculating checksum for '{}'", path);

        try (var input = Files.newInputStream(path)) {
            return DigestUtils.sha256Hex(new BufferedInputStream(input));
        }
    }

    @Override
    public long getFileSize(@NonNull Path path) throws IOException {
        log.debug("Getting file size for path '{}'", path);
        return Files.size(path);
    }

    @Override
    public long getPathSize(@NonNull Path path) throws IOException {
        return FileUtils.sizeOfDirectory(path.toFile());
    }

    @Override
    public ZipFile openZipFile(@NonNull Path path) throws IOException {
        log.debug("Opening zip file '{}'", path);
        return new ZipFile(path.toFile());
    }

    @Override
    public InputStream getEntryUnderBaseFolder(@NonNull ZipFile zipFile, @NonNull Path path) throws IOException {
        var entries = zipFile.stream()
                .filter(e -> {
                    var p = Path.of(e.getName());
                    return p.getNameCount() != 1 && p.subpath(1, p.getNameCount()).equals(path);
                })
                .toList();
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("No entry found for path: " + path);
        } else if (entries.size() > 1) {
            throw new IllegalArgumentException("Multiple entries found for path: " + path);
        }
        var entry = entries.get(0);
        log.debug("Requested entry for path '{}', found match on '{}'", path, entry.getName());
        return zipFile.getInputStream(entries.get(0));
    }

    @Override
    public Path addCreationTimeToFileName(Path path) throws IOException {
        var dveName = new DveFileName(path.getFileName().toString());
        if (dveName.isDve()) {
            if (dveName.getOrderNumber() == null) {
                var creationTime = getCreationTimeUnixTimestamp(path);
                var newFileName = creationTime + "-" + path.getFileName();
                return Files.move(path, path.getParent().resolve(newFileName));
            } else {
                log.debug("File '{}' already has a creation time in its name, not adding it again", path);
                return path;
            }
        } else {
            log.debug("File '{}' is not a DvE file, not adding creation time to filename", path);
            return path;
        }
    }

    private long getCreationTimeUnixTimestamp(Path path) {
        try {
            BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
            return attrs.creationTime().toInstant().toEpochMilli();
        } catch (IOException e) {
            throw new RuntimeException("Failed to get creation time of file: " + path, e);
        }
    }


    @Override
    public void cleanup(Path dir, Pattern pattern) throws IOException {
        log.debug("Cleaning up directory '{}' with pattern '{}'", dir, pattern);
        try (var stream = Files.list(dir)) {
            stream.filter(p -> pattern.matcher(p.getFileName().toString()).matches())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            log.error("Unable to delete file '{}'", p, e);
                        }
                    });
        }
    }
}
