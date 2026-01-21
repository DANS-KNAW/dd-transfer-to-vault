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
package nl.knaw.dans.transfer.core;

import lombok.Builder;
import lombok.NonNull;
import nl.knaw.dans.lib.util.healthcheck.DependenciesReadyCheck;
import nl.knaw.dans.lib.util.inbox.InboxTaskFactory;
import nl.knaw.dans.transfer.client.ValidateBagPackClient;
import nl.knaw.dans.transfer.client.VaultCatalogClient;

import java.net.URI;
import java.nio.file.Path;

@Builder
public class ExtractMetadataTaskFactory implements InboxTaskFactory {
    @NonNull
    private final String ocflStorageRoot;
    @NonNull
    private final Path outboxProcessed;
    @NonNull
    private final Path outboxFailed;
    @NonNull
    private final Path outboxRejected;
    @NonNull
    private final Path nbnRegistrationInbox;
    @NonNull
    private final URI vaultCatalogBaseUri;
    @NonNull
    private final DveMetadataReader dveMetadataReader;
    @NonNull
    private final FileService fileService;
    @NonNull
    private final VaultCatalogClient vaultCatalogClient;
    @NonNull
    private final ValidateBagPackClient validateBagPackClient;
    @NonNull
    private final DependenciesReadyCheck readyCheck;

    @Override
    public Runnable createInboxTask(Path targetNbnDir) {
        return new ExtractMetadataTask2(targetNbnDir, ocflStorageRoot, outboxProcessed, outboxFailed, outboxRejected,
            nbnRegistrationInbox, vaultCatalogBaseUri, dveMetadataReader, fileService,
            vaultCatalogClient, validateBagPackClient, readyCheck);
    }
}
