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

import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.FlatOmitPrefixLayoutConfig;
import io.dropwizard.hibernate.UnitOfWork;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.UUID;

public class TarTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TarTask.class);

    private final TransferItemDAO transferItemDAO;
    private final OcflRepositoryManager ocflRepositoryManager;
    private final String inboxPath;
    private final String workDir;
    private final String dataArchiveRoot;
    private final String tarCommand;

    public TarTask(TransferItemDAO transferItemDAO, OcflRepositoryManager ocflRepositoryManager, String inboxPath, String workDir, String dataArchiveRoot, String tarCommand) {
        this.transferItemDAO = transferItemDAO;
        this.ocflRepositoryManager = ocflRepositoryManager;
        this.inboxPath = inboxPath;
        this.workDir = workDir;
        this.dataArchiveRoot = dataArchiveRoot;
        this.tarCommand = tarCommand;
    }

    @Override
    public void run() {
        try {
            createTarArchive();
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void createTarArchive() throws IOException, InterruptedException {
        var workDir = Path.of(this.workDir);
        var uuid = UUID.randomUUID();

        log.info("created new UUID for archive with value {}", uuid);

        // move everything to a new folder, and reinitialize the ocflRepository as
        // atomically as possible
        log.info("moving all files to {}", workDir);
        var ocflRepoPath = moveOutboxToWorkdir(workDir, uuid);

        // run dmftar and upload results
        log.info("tarring directory {}/{}", workDir, uuid);
        tarDirectory(uuid, ocflRepoPath);

        // cleanup working directory
        log.info("cleaning up directory {}", ocflRepoPath);
        cleanupWorkingDirectory(ocflRepoPath);
    }

    private void cleanupWorkingDirectory(Path ocflRepoPath) throws IOException {
        FileUtils.deleteDirectory(ocflRepoPath.toFile());
    }

    @UnitOfWork
    public Path moveOutboxToWorkdir(Path workDir, UUID id) throws IOException {
        // get a list of all files and check if they are present
        var transferItems = transferItemDAO.findAllStatusTar();
        log.debug("retrieved {} TransferItem's to check", transferItems.size());

        var newPath = Path.of(workDir.toString(), id.toString());
        log.debug("moving ocfl repository to {}", newPath);
        ocflRepositoryManager.transferRepository(newPath);

        // TODO make this nicer
        var ocflRepository = new OcflRepositoryBuilder()
            .defaultLayoutConfig(new FlatOmitPrefixLayoutConfig().setDelimiter("urn:uuid:"))
            .storage(ocflStorageBuilder -> ocflStorageBuilder.fileSystem(newPath))
            .workDir(Inbox.OCFL_WORK_DIR)
            .build();

        // validate all the items in here
        for (var item : transferItems) {
            var objectId = item.getAipTarEntryName();

            if (ocflRepository.containsObject(objectId)) {
                item.setTransferStatus(TransferItem.TransferStatus.TARRING);
                item.setAipsTar(id.toString());
            }
        }

        return newPath;
    }

    // TODO proper error handling, make this nicer
    private void tarDirectory(UUID packageName, Path sourceDirectory) throws IOException, InterruptedException {
        var targetPackage = dataArchiveRoot + packageName.toString() + ".dfmtar";
        var cmd = String.format(tarCommand, targetPackage, sourceDirectory);
        log.debug("executing command {}", cmd);
        // TODO this is far from safe, spaces break this, so fix it
        var processBuilder = new ProcessBuilder(cmd.split(" "));

        // TODO see if we can get output asynchronously
        processBuilder.redirectOutput(ProcessBuilder.Redirect.PIPE);
        processBuilder.redirectError(ProcessBuilder.Redirect.PIPE);
        log.debug("waiting for command {}", cmd);
        var process = processBuilder.start();
        var result = process.waitFor();
        log.debug("received result from command: {}", result);
        var output = new String(process.getInputStream().readAllBytes(), Charset.defaultCharset());
        log.info(output);

        var errors = new String(process.getErrorStream().readAllBytes(), Charset.defaultCharset());
        log.info(errors);
    }

    private void cleanupTarFile(Path tarFile) throws IOException, InterruptedException {
        var processBuilder = new ProcessBuilder("dmftar", "--delete-archive", "-f", tarFile.toString());

        processBuilder.redirectOutput(ProcessBuilder.Redirect.PIPE);
        processBuilder.redirectError(ProcessBuilder.Redirect.PIPE);
        var process = processBuilder.start();
        var result = process.waitFor();
        var output = new String(process.getErrorStream().readAllBytes(), Charset.defaultCharset());
    }

}

