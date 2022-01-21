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

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.OcflRepository;
import nl.knaw.dans.ttv.db.TransferItemDAO;

public abstract class Task implements Runnable {

    protected final TransferItem transferItem;
    protected final TransferItemDAO transferItemDAO;
    protected final OcflRepositoryManager ocflRepository;
    protected final ObjectMapper objectMapper;

    protected Task(TransferItem transferItem, TransferItemDAO transferItemDAO, OcflRepositoryManager ocflRepository, ObjectMapper objectMapper) {
        this.transferItem = transferItem;
        this.transferItemDAO = transferItemDAO;
        this.ocflRepository = ocflRepository;
        this.objectMapper = objectMapper;
    }

    public TransferItem getTransferItem() {
        return transferItem;
    }
}
