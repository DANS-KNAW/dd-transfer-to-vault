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
package nl.knaw.dans.ttv.core;

import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.FlatOmitPrefixLayoutConfig;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class OcflRepositoryManagerImpl implements OcflRepositoryManager {
    private static final Logger log = LoggerFactory.getLogger(OcflRepositoryManager.class);

    private OcflRepository ocflRepository;

    public OcflRepositoryManagerImpl() {
        buildRepository();
    }

    private void buildRepository() {
        ocflRepository = new OcflRepositoryBuilder()
            .defaultLayoutConfig(new FlatOmitPrefixLayoutConfig().setDelimiter("urn:uuid:"))
            .storage(ocflStorageBuilder -> ocflStorageBuilder.fileSystem(Inbox.OCFL_STORAGE_ROOT))
            .workDir(Inbox.OCFL_WORK_DIR)
            .build();
    }

    @Override
    public synchronized OcflRepository getOcflRepository() {
        return ocflRepository;
    }

    @Override
    public synchronized void transferRepository(Path newPath) throws IOException {
        log.debug("moving directory {} to new directory {}", Inbox.OCFL_STORAGE_ROOT, newPath);
        // move all the contents
        moveDirectory(Inbox.OCFL_STORAGE_ROOT, newPath);

        // clean all remaining files from this directory
        log.debug("cleaning directory {}", Inbox.OCFL_STORAGE_ROOT);
        cleanDirectory(Inbox.OCFL_STORAGE_ROOT);

        // re-create ocfl repository for new additions
        log.debug("creating new repository in location {}", Inbox.OCFL_STORAGE_ROOT);
        buildRepository();
    }

    private void moveDirectory(Path sourceDirectory, Path targetDirectory) throws IOException {
        Files.walk(sourceDirectory).forEach(p -> {
            var relativePath = sourceDirectory.relativize(p);
            var finalPath = Path.of(targetDirectory.toString(), relativePath.toString());

            if (p.toFile().isDirectory()) {
                log.trace("creating directory '{}' because '{}' is a directory", finalPath, p);
                finalPath.toFile().mkdirs();
            }
            else {
                try {
                    var newPath = Path.of(targetDirectory.toString(), relativePath.toString());
                    log.trace("moving '{}' to '{}'", p, newPath);
                    Files.move(p, newPath);
                }
                catch (IOException e) {
                    log.error("unable to move file {}", p, e);
                }
            }
        });
    }

    private void cleanDirectory(Path sourceDirectory) throws IOException {
        FileUtils.cleanDirectory(sourceDirectory.toFile());
    }
}
