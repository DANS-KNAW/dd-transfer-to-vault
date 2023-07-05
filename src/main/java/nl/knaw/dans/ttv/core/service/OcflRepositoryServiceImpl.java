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

import io.ocfl.api.OcflOption;
import io.ocfl.api.OcflRepository;
import io.ocfl.api.model.ObjectVersionId;
import io.ocfl.api.model.VersionInfo;
import nl.knaw.dans.ttv.db.TransferItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.regex.Pattern;

public class OcflRepositoryServiceImpl implements OcflRepositoryService {
    private static final Logger log = LoggerFactory.getLogger(OcflRepositoryServiceImpl.class);

    private final FileService fileService;
    private final OcflRepositoryFactory ocflRepositoryFactory;

    public OcflRepositoryServiceImpl(FileService fileService, OcflRepositoryFactory ocflRepositoryFactory) {
        this.fileService = fileService;
        this.ocflRepositoryFactory = ocflRepositoryFactory;
    }

    @Override
    public OcflRepository openRepository(Path path) throws IOException {
        var workingDir = getWorkingDir(path);

        fileService.ensureDirectoryExists(path);
        fileService.ensureDirectoryExists(workingDir);

        log.trace("Opening OCFL repository on location '{}' and working dir '{}'", path, workingDir);
        return ocflRepositoryFactory.createRepository(path, workingDir);
    }

    Path getWorkingDir(Path path) {
        var parent = path.normalize().getParent();
        var name = "." + path.getFileName() + ".wd";

        if (parent == null) {
            return Path.of(name);
        }

        return parent.resolve(name);
    }

    @Override
    public String importTransferItem(OcflRepository ocflRepository, TransferItem transferItem) {
        var objectId = getObjectIdForBagId(transferItem.getBagId());
        var source = Objects.requireNonNull(Path.of(transferItem.getDveFilePath()), "dveFilePath can't be null: " + transferItem.getDveFilePath());

        log.debug("Importing file '{}' with objectId '{}' into OCFL repository", source, objectId);
        ocflRepository.putObject(
            ObjectVersionId.head(objectId),
            // TODO this will not work because all versions are required to exist, just inserting a random ID does not work
            //            ObjectVersionId.version(objectId, transferItem.getOcflObjectVersion()),
            source,
            new VersionInfo().setMessage("initial commit"),
            OcflOption.MOVE_SOURCE
        );

        return objectId;
    }

    @Override
    public String getObjectIdForBagId(String bagId) {
        Objects.requireNonNull(bagId, "Bag ID can't be null");
        bagId = bagId.strip();
        var idPattern = Pattern.compile("^urn:uuid:[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{12}$");
        var matcher = idPattern.matcher(bagId);

        if (!matcher.matches()) {
            throw new IllegalArgumentException(String.format("Value %s does not match expected pattern", bagId));
        }

        return bagId.substring(0, 9) + bagId.substring(9, 11) + "/" + bagId.substring(11, 13) + "/" + bagId.substring(13, 15) + "/" + bagId.substring(15);
    }

    @Override
    public void closeOcflRepository(OcflRepository ocflRepository, Path path) throws IOException {
        var workingDir = getWorkingDir(path);
        fileService.deleteDirectory(workingDir);
        ocflRepository.close();
    }

}
