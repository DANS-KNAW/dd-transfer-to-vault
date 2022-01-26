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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileServiceImpl implements FileService {
    private static final Logger log = LoggerFactory.getLogger(FileServiceImpl.class);

    @Override
    public Path moveFile(Path current, Path newPath) throws IOException {
        log.trace("moving file from {} to {}", current, newPath);
        return Files.move(current, newPath);
    }

    @Override
    public boolean deleteFile(Path path) throws IOException {
        log.trace("deleting file {}", path);
        return Files.deleteIfExists(path);
    }

    @Override
    public void deleteDirectory(Path path) throws IOException {
        FileUtils.deleteDirectory(path.toFile());
    }

    @Override
    public Object getFilesystemAttribute(Path path, String property) throws IOException {
        return Files.getAttribute(path, property);
    }

    @Override
    public String calculateChecksum(Path path) throws IOException {
        log.trace("calculating checksum for {}", path);
        return new DigestUtils("SHA-256").digestAsHex(Files.readAllBytes(path));
    }

    @Override
    public long getFileSize(Path path) throws IOException {
        log.trace("getting file size for path {}", path);
        return Files.size(path);
    }

    @Override
    public long getPathSize(Path path) throws IOException {
        return Files.walk(path).filter(Files::isRegularFile).map(p -> {
            try {
                var size = Files.size(p);
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
        return Files.createDirectories(path);
    }

}
