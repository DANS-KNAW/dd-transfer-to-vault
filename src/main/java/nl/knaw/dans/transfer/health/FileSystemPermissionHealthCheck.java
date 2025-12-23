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
package nl.knaw.dans.transfer.health;

import com.codahale.metrics.health.HealthCheck;
import nl.knaw.dans.transfer.config.NbnRegistrationConfig;
import nl.knaw.dans.transfer.config.TransferConfig;
import nl.knaw.dans.transfer.core.FileService;

import java.nio.file.Path;
import java.util.List;

public class FileSystemPermissionHealthCheck extends HealthCheck {
    private final TransferConfig transferConfig;
    private final NbnRegistrationConfig nbnRegistrationConfig;
    private final FileService fileService;

    public FileSystemPermissionHealthCheck(TransferConfig transferConfig, NbnRegistrationConfig nbnRegistrationConfig, FileService fileService) {
        this.transferConfig = transferConfig;
        this.nbnRegistrationConfig = nbnRegistrationConfig;
        this.fileService = fileService;
    }

    @Override
    protected Result check() throws Exception {
        var result = Result.builder();
        var isValid = true;
        var needReadWriteRights = List.of(
            List.of(transferConfig.getCollectDve().getInbox().getPath(),
                transferConfig.getCollectDve().getOutbox().getProcessed(),
                transferConfig.getCollectDve().getOutbox().getFailed()),
            List.of(transferConfig.getExtractMetadata().getInbox().getPath(),
                transferConfig.getExtractMetadata().getOutbox().getProcessed(),
                transferConfig.getExtractMetadata().getOutbox().getFailed(),
                transferConfig.getExtractMetadata().getOutbox().getRejected()),
            List.of(transferConfig.getSendToVault().getInbox().getPath(),
                transferConfig.getSendToVault().getOutbox().getProcessed(),
                transferConfig.getSendToVault().getOutbox().getFailed()),
            List.of(nbnRegistrationConfig.getInbox().getPath(),
                nbnRegistrationConfig.getOutbox().getProcessed(),
                nbnRegistrationConfig.getOutbox().getFailed())
        );

        for (var pathList : needReadWriteRights) {
            for (var path : pathList) {
                if (!fileService.canWriteTo(path)) {
                    result.withDetail(path.toString(), "Path is not writable");
                    isValid = false;
                }
            }
            if (!fileService.isSameFileSystem(pathList.toArray(new Path[0]))) {
                var p = String.join(", ", pathList.stream().map(Path::toString).toList());
                result.withDetail(p, "Paths are not on the same file system");
                isValid = false;
            }
        }
        if (isValid) {
            return result.healthy().build();
        }
        else {
            return result.unhealthy().withMessage("Some in/out-boxes have insufficient rights").build();
        }
    }
}
