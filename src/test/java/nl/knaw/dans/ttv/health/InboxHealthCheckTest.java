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

import nl.knaw.dans.ttv.DdTransferToVaultConfiguration;
import nl.knaw.dans.ttv.core.config.CollectConfiguration;
import nl.knaw.dans.ttv.core.service.FileService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InboxHealthCheckTest {

    @Test
    void checkValid() throws Exception {
        var fileService = Mockito.mock(FileService.class);
        var config = new DdTransferToVaultConfiguration();
        config.setCollect(new CollectConfiguration());
        config.getCollect().setInboxes(List.of(
            new CollectConfiguration.InboxEntry("a", Path.of("a")),
            new CollectConfiguration.InboxEntry("b", Path.of("b"))
        ));

        Mockito.when(fileService.canRead(Mockito.any())).thenReturn(true);
        Mockito.when(fileService.exists(Mockito.any())).thenReturn(true);

        var result = new InboxHealthCheck(config, fileService).check();

        assertTrue(result.isHealthy());
    }

    @Test
    void checkInvalid() throws Exception {
        var fileService = Mockito.mock(FileService.class);
        var config = new DdTransferToVaultConfiguration();
        config.setCollect(new CollectConfiguration());
        config.getCollect().setInboxes(List.of(
            new CollectConfiguration.InboxEntry("a", Path.of("a")),
            new CollectConfiguration.InboxEntry("b", Path.of("b"))
        ));

        Mockito.when(fileService.canRead(Mockito.any())).thenReturn(false).thenReturn(true);
        Mockito.when(fileService.exists(Mockito.any())).thenReturn(true).thenReturn(false);

        var result = new InboxHealthCheck(config, fileService).check();

        assertFalse(result.isHealthy());
    }

}
