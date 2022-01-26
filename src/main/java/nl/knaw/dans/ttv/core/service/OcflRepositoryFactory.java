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

import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.FlatOmitPrefixLayoutConfig;

import java.nio.file.Path;

public class OcflRepositoryFactory {
    public OcflRepository createRepository(Path storageDirectory, Path workingDirectory) {
        return new OcflRepositoryBuilder()
            .defaultLayoutConfig(new FlatOmitPrefixLayoutConfig().setDelimiter("urn:uuid:"))
            .storage(ocflStorageBuilder -> ocflStorageBuilder.fileSystem(storageDirectory))
            .workDir(workingDirectory)
            .build();
    }
}
