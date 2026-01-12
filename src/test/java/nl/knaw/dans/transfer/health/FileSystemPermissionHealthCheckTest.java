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

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.knaw.dans.transfer.config.NbnRegistrationConfig;
import nl.knaw.dans.transfer.config.TransferConfig;
import nl.knaw.dans.transfer.core.FileService;
import nl.knaw.dans.transfer.core.FileServiceImpl;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class FileSystemPermissionHealthCheckTest {

    private static final String NBN_BOXES = """
        {
          "inbox": {"path": "a"},
          "outbox": {
            "processed": "pb/b",
            "failed": "pc/c"
          }
        }
        """;
    private static final String TRANSFER_BOXES = """
        {
            "collectDve": {
                "inbox": {"path": "d"},
                "outbox": {
                "processed": "pe/e",
                "failed": "pf/f"
                }
            },
            "extractMetadata": {
                "inbox": {"path": "g"},
                "outbox": {
                "processed": "h",
                "failed": "i",
                "rejected": "j"
                }
            },
            "sendToVault": {
                "inbox": {"path": "k"},
                "outbox": {
                "processed": "l",
                "failed": "m"
                }
            }
            }
        """;

    @Test
    void checkNothingExists() throws Exception {
        FileService fileService = new FileServiceImpl();

        ObjectMapper mapper = new ObjectMapper();

        var result = new FileSystemPermissionHealthCheck(
            mapper.readValue(TRANSFER_BOXES, TransferConfig.class),
            mapper.readValue(NBN_BOXES, NbnRegistrationConfig.class),
            fileService
        ).check();

        assertFalse(result.isHealthy());
        assertEquals(17, result.getDetails().size());
        assertEquals("Paths are not on the same file system", result.getDetails().get("g, h, i, j"));
        assertEquals("Path is not writable", result.getDetails().get("k"));
    }

    @Test
    void checkEverythingWorks() throws Exception {
        FileService mock = mock(FileService.class);
        when(mock.canWriteTo(any(Path.class))).thenReturn(true);
        when(mock.isSameFileSystem(any(Path[].class))).thenReturn(true);

        ObjectMapper mapper = new ObjectMapper();

        var result = new FileSystemPermissionHealthCheck(
            mapper.readValue(TRANSFER_BOXES, TransferConfig.class),
            mapper.readValue(NBN_BOXES, NbnRegistrationConfig.class),
            mock
        ).check();

        assertTrue(result.isHealthy());
        assertEquals(0, result.getDetails().size());
    }
}
