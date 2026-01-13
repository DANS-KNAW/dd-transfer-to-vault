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
import nl.knaw.dans.transfer.TestDirFixture;
import nl.knaw.dans.transfer.config.NbnRegistrationConfig;
import nl.knaw.dans.transfer.config.TransferConfig;
import nl.knaw.dans.transfer.core.FileService;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FileSystemPermissionHealthCheckTest extends TestDirFixture {

    private final String NBN_BOXES = String.format("""
        {
          "inbox": {"path": "a"},
          "outbox": {
            "processed": "%s",
            "failed": "%s"
          }
        }
        """,
        testDir.resolve("pb/b"),
        testDir.resolve("pc/c")
    );
    private final String TRANSFER_BOXES = String.format("""
        {
            "collectDve": {
                "inbox": {"path": "d"},
                "outbox": {
                    "processed": "%s",
                    "failed": "%s"
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
                },
                "dataVault": {
                    "currentBatchWorkingDir": "n",
                    "batchRoot": "o"
                }
            }
        }
        """,
        testDir.resolve("pe/e"),
        testDir.resolve("pf/f")
    );

    @Test
    void checkNothingExists() throws Exception {
        for (var dir : new String[] { "pb", "pc", "pe", "pf" }) {
            testDir.resolve(dir).toFile().mkdirs();
        }
        FileService fileService = mock(FileService.class);
        when(fileService.canWriteTo(any(Path.class))).thenReturn(false);
        when(fileService.isSameFileSystem(any(Path[].class))).thenReturn(false);

        ObjectMapper mapper = new ObjectMapper();

        var result = new FileSystemPermissionHealthCheck(
            mapper.readValue(TRANSFER_BOXES, TransferConfig.class),
            mapper.readValue(NBN_BOXES, NbnRegistrationConfig.class),
            fileService
        ).check();

        assertFalse(result.isHealthy());
        assertEquals(20, result.getDetails().size());
        assertEquals("Path is not writable", result.getDetails().get("k"));
        assertEquals("Paths are not on the same file system", result.getDetails().get(String.format("d, %s/pf", testDir)));
    }

    @Test
    void checkEverythingWorks() throws Exception {
        for (var dir : new String[] { "pb", "pc", "pe", "pf" }) {
            testDir.resolve(dir).toFile().mkdirs();
        }
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
