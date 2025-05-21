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

import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
@AllArgsConstructor
public class RemoveEmptyTargetDirsTask implements Runnable {
    private final Path path;

    @Override
    public void run() {
        log.debug("Deleting empty subdirs in: {}", path);
        try (var stream = Files.list(path)) {
            stream
                .filter(Files::isDirectory)
                .forEach(this::deleteIfEmpty);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to list subdirs in: " + path, e);
        }
    }

    private void deleteIfEmpty(Path subdir) {
        try (var stream = Files.list(subdir)) {
            if (stream.findAny().isEmpty()) {
                log.debug("Deleting empty subdir: {}", subdir);
                Files.delete(subdir);
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to delete empty subdir: " + subdir, e);
        }
    }
}
