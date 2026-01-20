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
import nl.knaw.dans.transfer.client.DataVaultClient;

import java.nio.file.Path;

@Builder
public class SendToVaultFlushTaskFactory {
    @NonNull
    private final Path currentBatchWorkDir;
    @NonNull
    private final Path dataVaultBatchRoot;
    @NonNull
    private final DataVaultClient dataVaultClient;
    @NonNull
    private final DependenciesReadyCheck readyCheck;

    public Runnable create() {
        return new SendToVaultFlushTask(currentBatchWorkDir, dataVaultBatchRoot, dataVaultClient, readyCheck);
    }
}
