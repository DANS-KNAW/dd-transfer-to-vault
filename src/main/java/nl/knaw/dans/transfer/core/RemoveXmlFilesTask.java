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

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.nio.file.Files;
import java.nio.file.Path;

@AllArgsConstructor
@Slf4j
public class RemoveXmlFilesTask implements Runnable {
    private final Path path;

    @Override
    public void run() {
        log.debug("Deleting XML files in: {}", path);
        try (var stream = Files.list(path)) {
            stream
                .filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().endsWith(".xml"))
                .map(Path::toFile)
                .forEach(FileUtils::deleteQuietly);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to list files in: " + path, e);
        }

    }
}
