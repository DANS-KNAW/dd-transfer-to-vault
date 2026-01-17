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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.UUID;
import java.util.zip.ZipFile;

public class FileServiceImpl implements FileService {
    private static final Logger log = LoggerFactory.getLogger(FileServiceImpl.class);

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
    public void moveAtomically(@NonNull Path oldLocation, @NonNull Path newLocation) throws IOException {
        Files.move(oldLocation, newLocation, StandardCopyOption.ATOMIC_MOVE);
        fsyncDirectory(oldLocation.getParent());
        fsyncDirectory(newLocation.getParent());
    }

    @Override
    public void fsyncFile(Path file) throws IOException {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
            ch.force(true);
        }
    }

    @Override
    public void fsyncDirectory(Path dir) throws IOException {
        // Works on Unix-like systems. On some platforms/filesystems this can throw; if it does,
        // propagate (safer) or change to best-effort depending on your needs.
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        }
    }

    @Override
    public boolean exists(Path path) {
        return Files.exists(path);
    }

    @Override
    public boolean isSameFileSystem(java.util.Collection<Path> paths) {
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
    public boolean canWriteTo(Path path) {
        if (!Files.exists(path) || !Files.isDirectory(path) || !Files.isWritable(path)) {
            // without this check deleteIfExists may cause AccessDeniedException
            // the rest is copied from dd-sword2
            return false;
        }

        var filename = path.resolve(String.format(".%s", UUID.randomUUID()));

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
                log.error("Unable to delete file due to IO error", e);
            }
        }
    }

    @Override
    public void ensureDirectoryExists(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
        }
        // Wait for dir to be detected
        while (!Files.isDirectory(dir)) {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                log.warn("Directory was created, but still not visible: {}", dir);
            }
        }
    }
}
