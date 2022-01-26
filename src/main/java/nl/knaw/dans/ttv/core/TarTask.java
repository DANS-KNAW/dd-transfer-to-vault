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

import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

public class TarTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TarTask.class);

    private final TransferItemService transferItemService;
    private final Path inboxPath;
    private final String dataArchiveRoot;
    private final String tarCommand;
    private final UUID uuid;
    private final TarCommandRunner tarCommandRunner;

    public TarTask(TransferItemService transferItemService, UUID uuid, Path inboxPath, String dataArchiveRoot, String tarCommand, TarCommandRunner tarCommandRunner) {
        this.transferItemService = transferItemService;
        this.inboxPath = inboxPath;
        this.dataArchiveRoot = dataArchiveRoot;
        this.tarCommand = tarCommand;
        this.uuid = uuid;
        this.tarCommandRunner = tarCommandRunner;
    }

    @Override
    public void run() {
        try {
            createTarArchive();
        }
        catch (IOException | InterruptedException e) {
            log.error("error while creating TAR archive", e);
        }
    }

    private void createTarArchive() throws IOException, InterruptedException {
        // run dmftar and upload results
        log.info("tarring directory {} with UUID {}", this.inboxPath, uuid);
        tarDirectory(uuid, this.inboxPath);

        transferItemService.updateToCreatedForTarId(uuid.toString());
    }

    private void tarDirectory(UUID packageName, Path sourceDirectory) throws IOException, InterruptedException {
        var targetPackage = dataArchiveRoot + packageName.toString() + ".dmftar";
        var result = tarCommandRunner.tarDirectory(sourceDirectory, targetPackage);

        log.debug("tar command return code: {}", result.getStatusCode());
        log.debug("tar command output: '{}'", result.getStdout());
        log.debug("tar command errors: '{}'", result.getStderr());

        // it failed
        if (result.getStatusCode() != 0) {
            throw new IOException(String.format("unable to tar folder '%s' to location '%s", sourceDirectory, targetPackage));
        }
    }

}

