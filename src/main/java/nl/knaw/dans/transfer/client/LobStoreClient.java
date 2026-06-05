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
package nl.knaw.dans.transfer.client;

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lobstore.client.api.TransferRequestDto;
import nl.knaw.dans.lobstore.client.resources.DefaultApi;

import java.util.List;

@Slf4j
public class LobStoreClient {
    private final DefaultApi api;

    public LobStoreClient(DefaultApi api) {
        this.api = api;
    }

    public void requestTransfers(List<TransferRequestDto> requests) {
        if (requests.isEmpty()) {
            return;
        }

        log.debug("Requesting dd-lob-store to process {} large objects", requests.size());
        try {
            var results = api.addTransfers(requests);
            log.debug("dd-lob-store processed {} requests", results.size());
        }
        catch (Exception e) {
            log.error("Error communicating with dd-lob-store", e);
        }
    }
}
