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
import java.util.Objects;
import java.util.zip.ZipFile;

public class FileServiceImpl implements FileService {
    private static final Logger log = LoggerFactory.getLogger(FileServiceImpl.class);

    @Override
    public Path moveFile(Path current, Path newPath) throws IOException {
        Objects.requireNonNull(current, "current path cannot be null");
        Objects.requireNonNull(newPath, "newPath cannot be null");
        log.trace("moving file from {} to {}", current, newPath);

        if (Files.exists(newPath)) {
            throw new FileAlreadyExistsException(String.format("Cannot move file %s to %s, file already exists", current, newPath));
        }

        return Files.move(current, newPath);
    }

    @Override
    public Path moveFileAtomically(Path filePath, Path newPath) throws IOException {
        Objects.requireNonNull(filePath, "filePath cannot be null");
        Objects.requireNonNull(newPath, "newPath cannot be null");

        var tempTarget = newPath.getParent().resolve(newPath.getFileName() + ".part");

        var store1 = Files.getFileStore(filePath);
        var store2 = Files.getFileStore(newPath.getParent());

        if (store1.equals(store2)) {
            log.info("Moving file {} to {}", filePath, newPath);
            return moveFile(filePath, newPath);
        }

        // there could be leftovers from a previous attempt, remove them
        Files.deleteIfExists(tempTarget);

        log.info("Moving file atomically {} to {}", filePath, newPath);
        moveFile(filePath, tempTarget);
        return moveFile(tempTarget, newPath);
    }

    @Override
    public void ensureDirectoryExists(Path path) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");
        Files.createDirectories(path);
    }

    /**
     * Moves a file to the rejected/ subfolder of where it was initially found If a file already exists, it will append a number to the filename.
     *
     * It also writes an error description inside a file which is named `filename`.error.txt (or `filename`.1.error.txt` if it already exists)
     *
     * @param path The path to the file to reject
     */
    @Override
    public void rejectFile(Path path, Exception exception) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");
        Objects.requireNonNull(exception, "exception cannot be null");

        var rejectedFolder = path.getParent().resolve("rejected");
        ensureDirectoryExists(rejectedFolder);

        var filename = path.getFileName().toString();

        log.debug("Moving rejected file to '{}'", rejectedFolder);
        var extension = FilenameUtils.getExtension(filename);
        var fileBaseName = FilenameUtils.removeExtension(filename);

        log.trace("File base name is '{}', extension is '{}'", fileBaseName, extension);
        var rejectedFileName = fileBaseName + "." + extension;
        var errorFileName = fileBaseName + ".error.txt";

        var duplicateCounter = 1;

        while (Files.exists(rejectedFolder.resolve(rejectedFileName))) {
            log.trace("File '{}' already exists, generating a new filename", rejectedFolder.resolve(rejectedFileName));
            rejectedFileName = fileBaseName + "_" + duplicateCounter + "." + extension;
            errorFileName = fileBaseName + "_" + duplicateCounter + ".error.txt";

            duplicateCounter += 1;
        }

        log.trace("Settled on '{}' for filename", rejectedFileName);
        var targetPath = rejectedFolder.resolve(rejectedFileName);
        var targetErrorPath = rejectedFolder.resolve(errorFileName);

        log.trace("Moving file to '{}', writing error report to '{}'", targetPath, targetErrorPath);

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
    public boolean canWrite(Path path) {
        return Files.isWritable(path);
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
        return Files.getFileStore(path);
    }

    void writeExceptionToFile(Path errorReportName, Exception exception) throws IOException {
        var writer = new StringWriter();
        var pw = new PrintWriter(writer);
        exception.printStackTrace(pw);
        var stackTrace = writer.toString();
        pw.close();

        Files.write(errorReportName, stackTrace.getBytes());
    }

    @Override
    public boolean deleteFile(Path path) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");
        log.trace("Deleting file {}", path);
        return Files.deleteIfExists(path);
    }

    @Override
    public void deleteDirectory(Path path) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");
        log.trace("Deleting directory '{}'", path);
        FileUtils.deleteDirectory(path.toFile());
    }

    @Override
    public Object getFilesystemAttribute(Path path, String property) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");
        Objects.requireNonNull(property, "property cannot be null");
        log.trace("Getting attribute {} for path '{}'", property, path);
        return Files.getAttribute(path, property);
    }

    @Override
    public String calculateChecksum(Path path) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");
        log.trace("Calculating checksum for '{}'", path);

        try (var input = Files.newInputStream(path)) {
            return DigestUtils.sha256Hex(new BufferedInputStream(input));
        }
    }

    @Override
    public long getFileSize(Path path) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");
        log.trace("Getting file size for path '{}'", path);
        return Files.size(path);
    }

    @Override
    public long getPathSize(Path path) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");

        try (var files = Files.walk(path)) {
            return files
                .filter(Files::isRegularFile)
                .mapToLong(p -> {
                    try {
                        var size = getFileSize(p);
                        log.trace("File size for file '{}' is {} bytes", p, size);
                        return size;
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .sum();
        }
    }

    @Override
    public ZipFile openZipFile(Path path) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");
        log.trace("Opening zip file '{}'", path);
        return new ZipFile(path.toFile());
    }

    @Override
    public InputStream openFileFromZip(ZipFile zipFile, Path path) throws IOException {
        Objects.requireNonNull(zipFile, "zipFile cannot be null");
        Objects.requireNonNull(path, "path cannot be null");

        var entryPath = Objects.requireNonNull(zipFile.stream()
                .filter(e -> e.getName().endsWith(path.toString()))
                .findFirst()
                .orElse(null)
            , String.format("no entries found for path '%s' in zip file %s", path, zipFile)
        );

        log.trace("Requested entry for path '{}', found match on '{}'", path, entryPath);

        return zipFile.getInputStream(entryPath);
    }

}
