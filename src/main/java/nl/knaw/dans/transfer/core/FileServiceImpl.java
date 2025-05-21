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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
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
}
