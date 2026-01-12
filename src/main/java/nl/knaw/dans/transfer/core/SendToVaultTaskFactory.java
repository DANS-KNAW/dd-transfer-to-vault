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

import io.dropwizard.util.DataSize;
import lombok.Builder;
import nl.knaw.dans.lib.util.inbox.InboxTaskFactory;
import nl.knaw.dans.transfer.client.DataVaultClient;
import nl.knaw.dans.transfer.config.CustomPropertyConfig;

import java.nio.file.Path;
import java.util.Map;

@Builder
public class SendToVaultTaskFactory implements InboxTaskFactory {
    private final Path currentBatchWorkDir;
    private final Path dataVaultBatchRoot;
    private final DataSize batchThreshold;
    private final Path outboxProcessed;
    private final Path outboxFailed;
    private final DataVaultClient dataVaultClient;
    private final String defaultMessage;
    private final Map<String, CustomPropertyConfig> customProperties;

    @Override
    public Runnable createInboxTask(Path path) {
        return new SendToVaultTask(path, currentBatchWorkDir, dataVaultBatchRoot, batchThreshold, outboxProcessed, outboxFailed, dataVaultClient, defaultMessage, customProperties);
    }
}
