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
import nl.knaw.dans.ttv.db.TransferItem;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface OcflRepositoryService {

    OcflRepository createRepository(Path path, String id) throws IOException;

    void moveTransferItemsToRepository(OcflRepository ocflRepository, List<TransferItem> transferItems);

    String importTransferItem(OcflRepository ocflRepository, TransferItem transferItem);

    void closeOcflRepository(OcflRepository ocflRepository);

    void cleanupRepository(Path workingDir, String id) throws IOException;
}
