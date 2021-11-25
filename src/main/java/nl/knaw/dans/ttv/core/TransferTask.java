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

import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferTask extends Task {

    private static final Logger log = LoggerFactory.getLogger(TransferTask.class);


    public TransferTask(TransferItem transferItem, TransferItemDAO transferItemDAO) {
        super(transferItem, transferItemDAO);
    }

    @Override
    public void run() {
        log.info("Running task" + this);
        extractMetadata();
    }

    public void extractMetadata() {
        //TODO pre condition: transfer-inbox contains Dataset-Version-Exports (DVE) as exported from the Data Station, without the additional files
        //

        /* TODO Extract PID and version from DVE name
         * TODO Using PID and version, obtain dataset metadata, including Data Vault extension metadata, from DVE oai-ore.jsonld. This yields: DV PID, DV PID version, bagID, NBN, Other ID, Other ID version, SWORD token from Data Vault extension metadata
         * TODO Obtain Source DV instance from directory in which DVE resides
         * TODO Compute hash over zipped DVE to use as BagChecksum
         * TODO Update TransferItem with all the extracted metadata. Update the status to MOVE
         *  */

        //TODO post condition: TransferItem created, xml-file deleted
    }
}
