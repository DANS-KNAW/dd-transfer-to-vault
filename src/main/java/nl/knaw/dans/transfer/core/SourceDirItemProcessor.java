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

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;

/**
 * Consumer that processes files from a source directory until the producer removes the source directory.
 */
@AllArgsConstructor
@Slf4j
public abstract class SourceDirItemProcessor {
    private final Path srcDir;
    private final String itemType;

    private final Predicate<Path> filter;
    private final Comparator<Path> comparator;

    protected final FileService fileService;

    private final long delayBetweenProcessingRounds;

    public void processUntilRemoved() {
        try {
            if (isBlocked()) {
                log.debug("Source directory {} is blocked, skipping", srcDir);
                return;
            }

            var items = getItems();
            while (fileService.exists(srcDir)) {
                log.debug("Found {} {}s to process", items.size(), itemType);
                for (var item : items) {
                    processItem(item);
                }

                Thread.sleep(delayBetweenProcessingRounds);

                items = getItems();
            }
        }
        catch (IllegalArgumentException e) {
            log.warn("Invalid {}, skipping", itemType);
            try {
                blockTarget();
                rejectCurrentItem(e);
            }
            catch (IOException ioe) {
                log.error("Unable to block target directory", ioe);
            }
        }
        catch (Exception e) {
            log.error("Error processing " + itemType + " files", e);
            try {
                blockTarget();
                failCurrentItem(e);
            }
            catch (IOException ioe) {
                log.error("Unable to block target directory", ioe);
            }
        }
        finally {
            log.debug("Finished processing {}s in {}", itemType, srcDir);
        }
    }

    protected abstract void processItem(Path item) throws IOException;

    protected abstract void failCurrentItem(Exception e);

    protected abstract void rejectCurrentItem(IllegalArgumentException e);


    private List<Path> getItems() throws IOException {
        try (var dirStream = fileService.list(srcDir)) {
            return dirStream.filter(filter).sorted(comparator).toList();
        }
        catch (NoSuchFileException e) {
            log.debug("Source directory {} removed. No more items to process.", srcDir);
            return List.of();
        }
        catch (IOException e) {
            log.error("Error while reading source directory {}", srcDir, e);
            throw e;
        }
    }

    private boolean isBlocked() throws IOException {
        return fileService.exists(srcDir.resolve("block"));
    }

    private void blockTarget() throws IOException {
        var blockFile = srcDir.resolve("block");
        fileService.writeString(blockFile, "");
        fileService.fsyncFile(blockFile);
        fileService.fsyncDirectory(srcDir);
    }

}
