/*
 * Copyright (C) 2025 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.transfer.core;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.zip.ZipFile;

@Slf4j
public class FileServiceImpl implements FileService {
    private static final String ERROR_LOG_SUFFIX = "-error.log";

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
        }
        else if (entries.size() > 1) {
            throw new IllegalArgumentException("Multiple entries found for path: " + path);
        }
        var entry = entries.get(0);
        log.debug("Requested entry for path '{}', found match on '{}'", path, entry.getName());
        return zipFile.getInputStream(entries.get(0));
    }

    @Override
    public InputStream newInputStream(@NonNull Path path) throws IOException {
        return Files.newInputStream(path);
    }

    @Override
    public Stream<Path> list(@NonNull Path dir) throws IOException {
        return Files.list(dir);
    }

    @Override
    public void writeString(@NonNull Path path, @NonNull String content) throws IOException {
        Files.writeString(path, content, StandardCharsets.UTF_8);
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(@NonNull Path path, @NonNull Class<A> type) throws IOException {
        return Files.readAttributes(path, type);
    }

    @Override
    public Path move(@NonNull Path from, @NonNull Path to) throws IOException {
        if (isSameFileSystem(List.of(from, to.getParent()))) {
            Files.move(from, to, StandardCopyOption.ATOMIC_MOVE);
            // Ensure durability and visibility
            if (Files.isRegularFile(to)) {
                fsyncFile(to);
            }
            fsyncDirectory(to.getParent());
            if (!from.getParent().equals(to.getParent())) {
                try {
                    fsyncDirectory(from.getParent());
                }
                catch (NoSuchFileException e) {
                    // parent dir of 'from' no longer exists, nothing to fsync
                }
            }
            return to;
        }
        else {
            var targetDir = to.getParent();
            var temp = targetDir.resolve(to.getFileName().toString() + ".tmp");
            Files.copy(from, temp, StandardCopyOption.REPLACE_EXISTING);
            fsyncFile(temp);
            Files.move(temp, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            fsyncDirectory(targetDir);
            Files.delete(from);
            try {
                fsyncDirectory(from.getParent());
            }
            catch (NoSuchFileException e) {
                // parent dir of 'from' no longer exists, nothing to fsync
            }
            return to;
        }
    }

    @Override
    public void moveAndWriteErrorLog(@NonNull Path dve, @NonNull Path outbox, @NonNull Exception e) {
        log.error("Error moving file from {} to {}: {}", dve, outbox, e.getMessage(), e);
        try {
            move(dve, outbox.resolve(dve.getFileName()));
            var errorLog = outbox.resolve(dve.getFileName().toString() + ERROR_LOG_SUFFIX);
            Files.writeString(errorLog, e.getMessage() + System.lineSeparator()
                + ExceptionUtils.getStackTrace(e));
            fsyncFile(errorLog);
        }
        catch (IOException ex) {
            log.error("Error writing error log for file move from {} to {}: {}", dve, outbox, ex.getMessage(), ex);
        }
    }

    @Override
    public void delete(@NonNull Path path) throws IOException {
        Files.delete(path);
        fsyncDirectory(path.getParent());
    }

    @Override
    public void fsyncFile(@NonNull Path file) throws IOException {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
            ch.force(true);
        }
    }

    @Override
    public void fsyncDirectory(@NonNull Path dir) throws IOException {
        // Works on Unix-like systems. On some platforms/filesystems this can throw; if it does,
        // propagate (safer) or change to best-effort depending on your needs.
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        }
    }

    @Override
    public FileSystem newFileSystem(@NonNull Path path) throws IOException {
        return FileSystems.newFileSystem(path, (ClassLoader) null);
    }

    @Override
    public OutputStream newOutputStream(@NonNull Path path) throws IOException {
        return Files.newOutputStream(path);
    }

    @Override
    public void createDirectory(@NonNull Path dir) throws IOException {
        Files.createDirectory(dir);
        fsyncDirectory(dir.getParent());
    }

    @Override
    public boolean isRegularFile(@NonNull Path path) {
        return Files.isRegularFile(path);
    }

    @Override
    public boolean isDirectory(@NonNull Path path) {
        return Files.isDirectory(path);
    }

    @Override
    public boolean exists(@NonNull Path path) {
        return Files.exists(path);
    }

    @Override
    public boolean exists(@NonNull Path path, int retries, long retryDelayMillis) {
        if (Files.exists(path)) {
            return true;
        }

        for (int attempt = 0; attempt < retries; attempt++) {
            try {
                Thread.sleep(retryDelayMillis);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Files.exists(path);
            }

            if (Files.exists(path)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean isSameFileSystem(@NonNull Collection<Path> paths) {
        var fileStores = new HashSet<FileStore>();
        var result = true;
        for (var path : paths) {
            FileStore fileStore = null;
            try {
                fileStore = Files.getFileStore(path);
            }
            catch (IOException e) {
                result = false;
            }
            fileStores.add(fileStore);
        }

        return result && fileStores.size() == 1;
    }

    @Override
    public boolean canReadFrom(@NonNull Path path) {
        return Files.exists(path) && Files.isDirectory(path) && Files.isReadable(path);
    }

    @Override
    public boolean canWriteTo(@NonNull Path path) {
        if (!Files.exists(path) || !Files.isDirectory(path) || !Files.isWritable(path)) {
            // without this check deleteIfExists may cause AccessDeniedException
            // the rest is copied from dd-sword2
            return false;
        }

        var filename = path.resolve(".write-check-" + UUID.randomUUID());

        try {
            Files.write(filename, new byte[] {});
            return true;
        }
        catch (IOException e) {
            return false;
        }
        finally {
            try {
                Files.deleteIfExists(filename);
            }
            catch (IOException e) {
                log.error("Unable to delete {} due to IO error", filename, e);
            }
        }
    }

    @Override
    public void ensureDirectoryExists(@NonNull Path dir) throws IOException {
        if (!Files.exists(dir)) {
            createDirectory(dir);
        }
        fsyncDirectory(dir.getParent());
    }

    @Nullable
    private Path findExistingTargetDir(@NonNull String targetNbn, @NonNull Path destinationRoot) {
        try (var stream = Files.list(destinationRoot)) {
            return stream.filter(Files::isDirectory)
                .filter(dir -> dir.getFileName().toString().startsWith(targetNbn + "-"))
                .findFirst()
                .orElse(null);
        }
        catch (IOException e) {
            throw new IllegalStateException("Unable to list directories in destination root: " + destinationRoot, e);
        }
    }

    @Override
    public void moveToTargetFor(@NonNull Path dve, @NonNull Path outbox, @NonNull String targetNbn, boolean addTimestampToFileName) {
        var existingDir = findExistingTargetDir(targetNbn, outbox);
        String fileName = addTimestampToFileName
            ? new DveFileName(dve)
            .withCreationTime(getCreationTimeFromFilesystem(dve))
            .getFileName()
            : dve.getFileName().toString();

        try {
            if (existingDir != null) {
                move(dve, existingDir.resolve(findFreeName(existingDir, fileName)));
            }
            else {
                createAndMoveSafe(dve,
                    outbox.resolve(targetNbn + "-" + generateRandomString(6, "ABCDEFGHIJKLMNOPQRSTUVWXYZ")), fileName);
            }
        }
        catch (NoSuchFileException e) {
            log.debug("Existing directory for target NBN was deleted: {}, creating new directory", targetNbn);
            var newDir = outbox.resolve(targetNbn + "-" + generateRandomString(6, "ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
            createAndMoveSafe(dve, newDir, fileName);
        }
        catch (IOException e) {
            throw new IllegalStateException("Unable to move file to existing directory: " + existingDir, e);
        }
    }

    private String generateRandomString(int length, @NonNull String alphabet) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int randomIndex = (int) (Math.random() * alphabet.length());
            sb.append(alphabet.charAt(randomIndex));
        }
        return sb.toString();
    }

    private OffsetDateTime getCreationTimeFromFilesystem(@NonNull Path file) {
        try {
            var attrs = readAttributes(file, BasicFileAttributes.class);
            return attrs.creationTime().toInstant().atOffset(OffsetDateTime.now().getOffset());
        }
        catch (IOException ex) {
            throw new IllegalStateException("Unable to read file attributes for: " + file, ex);
        }
    }

    @Override
    public String findFreeName(@NonNull Path targetDir, @NonNull String fileName) {
        var dveFileName = new DveFileName(targetDir.resolve(fileName));
        int sequenceNumber = 1;
        while (exists(dveFileName.getPath())) {
            dveFileName = dveFileName.withIndex(sequenceNumber++);
        }
        return dveFileName.getPath().getFileName().toString();
    }

    /**
     * Creates outdir.tmp then moves file into it with move, then renames outdir.tmp to outdir.
     *
     * @param file   the file to move
     * @param outdir the directory to create
     */
    private void createAndMoveSafe(@NonNull Path file, @NonNull Path outdir, @NonNull String fileName) {
        try {
            var tmpOutDir = outdir.resolveSibling(outdir.getFileName() + ".tmp");
            createDirectory(tmpOutDir);
            move(file, tmpOutDir.resolve(fileName));
            move(tmpOutDir, outdir);
        }
        catch (IOException e) {
            throw new IllegalStateException("Unable to create and move file to new directory: " + outdir, e);
        }
    }

}
