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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
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
        return Files.move(current, newPath);
    }

    @Override
    public Path moveFileAtomically(Path filePath, Path newPath) throws IOException {
        var tempTarget = Path.of(newPath.toString() + ".part");

        // there could be leftovers from a previous attempt, remove them
        Files.deleteIfExists(tempTarget);

        moveFile(filePath, tempTarget);
        return moveFile(tempTarget, newPath);
    }

    @Override
    public void ensureDirectoryExists(Path errorPath) throws IOException {
        Files.createDirectories(errorPath);
    }

    /**
     * Moves a file to the rejected/ subfolder of where it was initially found
     * If a file already exists, it will append a number to the filename.
     *
     * It also writes an error description inside a file which is named
     * `filename`.error.txt (or `filename`.1.error.txt` if it already exists)
     *
     * @param path The path to the file to reject
     */
    @Override
    public void rejectFile(Path path, Exception exception) throws IOException {
        var rejectedFolder = path.getParent().resolve("rejected");
        ensureDirectoryExists(rejectedFolder);

        log.debug("moving rejected file to '{}'", rejectedFolder);
        var index = path.getFileName().toString().lastIndexOf(".");
        var extension = path.getFileName().toString().substring(index);
        var fileBaseName = path.getFileName().toString().substring(0, index);

        log.trace("file base name is '{}', extension is '{}'", extension, fileBaseName);
        var rejectedFileName = fileBaseName + extension;
        var errorFileName = fileBaseName + ".error.txt";

        var duplicateCounter = 1;

        while (Files.exists(Path.of(rejectedFolder.toString(), rejectedFileName))) {
            rejectedFileName = fileBaseName + "_" + duplicateCounter + extension;
            errorFileName = fileBaseName + "_" + duplicateCounter + ".error.txt";

            duplicateCounter += 1;
        }

        var targetPath =Path.of(rejectedFolder.toString(), rejectedFileName);
        var targetErrorPath = Path.of(rejectedFolder.toString(), errorFileName);

        log.trace("moving file to '{}', writing error report to '{}'", targetPath, targetErrorPath);
        Files.move(path, targetPath);
        writeExceptionToFile(targetErrorPath, exception);
    }

    void writeExceptionToFile(Path errorReportName, Exception exception) throws FileNotFoundException {
        var writer = new PrintWriter(errorReportName.toFile());
        exception.printStackTrace(writer);
        writer.close();
    }

    @Override
    public boolean deleteFile(Path path) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");
        log.trace("deleting file {}", path);
        return Files.deleteIfExists(path);
    }

    @Override
    public void deleteDirectory(Path path) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");
        FileUtils.deleteDirectory(path.toFile());
    }

    @Override
    public Object getFilesystemAttribute(Path path, String property) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");
        Objects.requireNonNull(property, "property cannot be null");
        return Files.getAttribute(path, property);
    }

    @Override
    public String calculateChecksum(Path path) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");
        log.trace("calculating checksum for {}", path);
        return new DigestUtils("SHA-256").digestAsHex(Files.readAllBytes(path));
    }

    @Override
    public long getFileSize(Path path) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");
        log.trace("getting file size for path {}", path);
        return Files.size(path);
    }

    @Override
    public long getPathSize(Path path) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");
        return Files.walk(path).filter(Files::isRegularFile).map(p -> {
            try {
                var size = getFileSize(p);
                log.trace("file size for file {} is {} bytes", p, size);
                return size;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).reduce(0L, Long::sum);
    }

    @Override
    public Path createDirectory(Path path) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");
        return Files.createDirectories(path);
    }

    @Override
    public ZipFile openZipFile(Path path) throws IOException {
        return new ZipFile(path.toFile());
    }

    @Override
    public InputStream openFileFromZip(ZipFile zipFile, Path path) throws IOException {
        var entryPath = Objects.requireNonNull(zipFile.stream()
                .filter(e -> e.getName().endsWith(path.toString()))
                .findFirst()
                .orElse(null)
            , String.format("no entries found for path '%s' in zip file %s", path, zipFile)
        );

        log.trace("requested entry for path '{}', found match on '{}'", path, entryPath);

        return zipFile.getInputStream(entryPath);
    }

}
