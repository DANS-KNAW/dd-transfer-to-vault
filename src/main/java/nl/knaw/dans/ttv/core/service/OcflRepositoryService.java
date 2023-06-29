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

import io.ocfl.api.OcflRepository;
import nl.knaw.dans.ttv.db.TransferItem;

import java.io.IOException;
import java.nio.file.Path;

public interface OcflRepositoryService {

    OcflRepository openRepository(Path path) throws IOException;

    String importTransferItem(OcflRepository ocflRepository, TransferItem transferItem);

    String getObjectIdForBagId(String bagId);

    void closeOcflRepository(OcflRepository ocflRepository, Path path) throws IOException;
}
