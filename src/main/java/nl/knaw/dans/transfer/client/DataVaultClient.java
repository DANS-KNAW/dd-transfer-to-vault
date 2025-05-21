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

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.datavault.client.api.ImportCommandDto;
import nl.knaw.dans.datavault.client.api.LayerStatusDto;
import nl.knaw.dans.datavault.client.resources.DefaultApi;

import java.nio.file.Path;

@Slf4j
@AllArgsConstructor
public class DataVaultClient {
    private final DefaultApi vaultApi;

    public void sendBatchToVault(Path batchPath) {
        try {
            var importCommand = new ImportCommandDto()
                .path(batchPath.toAbsolutePath().toString());
            vaultApi.importsPost(importCommand);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to send batch to Data Vault", e);
        }
    }

    public LayerStatusDto createNewLayer() {
        try {
            return vaultApi.layersPost();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create new layer in Data Vault", e);
        }
    }

    public long getTopLayerSize() {
        try {
            Long sizeInBytes = vaultApi.layersTopGet().getSizeInBytes();
            if (sizeInBytes == null) {
                throw new RuntimeException("Received null size for top layer from Data Vault");
            }
            return sizeInBytes;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to get top layer size from Data Vault", e);
        }
    }

}
