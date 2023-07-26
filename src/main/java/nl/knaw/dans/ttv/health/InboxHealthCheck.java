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

package nl.knaw.dans.ttv.health;

import com.codahale.metrics.health.HealthCheck;
import nl.knaw.dans.ttv.DdTransferToVaultConfiguration;
import nl.knaw.dans.ttv.core.service.FileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class InboxHealthCheck extends HealthCheck {
    private static final Logger log = LoggerFactory.getLogger(InboxHealthCheck.class);

    private final DdTransferToVaultConfiguration configuration;
    private final FileService fileService;

    public InboxHealthCheck(DdTransferToVaultConfiguration configuration, FileService fileService) {
        this.configuration = configuration;
        this.fileService = fileService;
    }

    @Override
    protected Result check() throws Exception {
        var problems = new ArrayList<String>();

        for (var inboxEntry : configuration.getCollect().getInboxes()) {
            if (!fileService.exists(inboxEntry.getPath())) {
                problems.add(inboxEntry.getPath() + ": does not exist");
            } else {
                var canRead = false;
                try {
                    canRead = fileService.canRead(inboxEntry.getPath(), 5);
                } catch (TimeoutException e) {
                    log.warn("Inbox path '{}' is not readable within 5 seconds", inboxEntry.getPath());
                }
                if (!canRead) {
                    problems.add(inboxEntry.getPath() + ": not readable or the NFS server is not responding within the timeout");
                }
            }
        }

        if (problems.isEmpty()) {
            return Result.healthy();
        } else {
            return Result.unhealthy(String.format("The following inboxes are not accessible: %s",
                    String.join(", ", problems)));
        }
    }
}
