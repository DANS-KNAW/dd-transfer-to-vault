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
package nl.knaw.dans.ttv.core.service;

import nl.knaw.dans.ttv.core.config.DataArchiveConfiguration;
import nl.knaw.dans.ttv.core.dto.ProcessResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class ArchiveStatusServiceImplTest {

    ProcessRunner processRunner;
    DataArchiveConfiguration dataArchiveConfiguration = new DataArchiveConfiguration("username", "hostname", "");

    @BeforeEach
    void setUp() {
        this.processRunner = Mockito.mock(ProcessRunner.class);
    }

    @Test
    void getFileStatus() {
        var output = "-r--r--r--  1 danstst0    133120 2022-01-24 16:41 (DUL) ff99d9fd-53ef-48f2-8672-a40a2c91f1c6.dfmtar/0000/ff99d9fd-53ef-48f2-8672-a40a2c91f1c6.dfmtar.tar\n"
            + "-r--r--r--  1 danstst0        85 2022-01-24 16:41 (N/A) ff99d9fd-53ef-48f2-8672-a40a2c91f1c6.dfmtar/0000/ff99d9fd-53ef-48f2-8672-a40a2c91f1c6.dfmtar.tar.chksum\n"
            + "-r--r--r--  1 danstst0      3011 2022-01-24 16:41 (REG) ff99d9fd-53ef-48f2-8672-a40a2c91f1c6.dfmtar/0000/ff99d9fd-53ef-48f2-8672-a40a2c91f1c6.dfmtar.tar.idx\n";

        var service = new ArchiveStatusServiceImpl(dataArchiveConfiguration, processRunner);

        try {
            Mockito.when(processRunner.run((String[]) Mockito.any()))
                .thenReturn(new ProcessResult(0, output));

            var result = service.getFileStatus("ff99d9fd-53ef-48f2-8672-a40a2c91f1c6");

            assertEquals(ArchiveStatusService.FileStatus.DUAL, result.get("ff99d9fd-53ef-48f2-8672-a40a2c91f1c6.dfmtar/0000/ff99d9fd-53ef-48f2-8672-a40a2c91f1c6.dfmtar.tar"));
            assertEquals(ArchiveStatusService.FileStatus.UNKNOWN, result.get("ff99d9fd-53ef-48f2-8672-a40a2c91f1c6.dfmtar/0000/ff99d9fd-53ef-48f2-8672-a40a2c91f1c6.dfmtar.tar.chksum"));
            assertEquals(ArchiveStatusService.FileStatus.REGULAR, result.get("ff99d9fd-53ef-48f2-8672-a40a2c91f1c6.dfmtar/0000/ff99d9fd-53ef-48f2-8672-a40a2c91f1c6.dfmtar.tar.idx"));
        }
        catch (Exception e) {
            fail(e);
        }
    }

    @Test
    void getFileStatusWithError() {
        var service = new ArchiveStatusServiceImpl(dataArchiveConfiguration, processRunner);

        try {
            var output = "command does not exist";

            Mockito.when(processRunner.run((String[]) Mockito.any()))
                .thenReturn(new ProcessResult(1, output));

            var error = assertThrows(IOException.class, () -> {
                var result = service.getFileStatus("ff99d9fd-53ef-48f2-8672-a40a2c91f1c6");
            });
        }
        catch (Exception e) {
            fail(e);
        }
    }


    @Test
    void unexpectedStatus() {
        var output = "-r--r--r--  1 danstst0    133120 2022-01-24 16:41 (ABC) ff99d9fd-53ef-48f2-8672-a40a2c91f1c6.dfmtar/0000/ff99d9fd-53ef-48f2-8672-a40a2c91f1c6.dfmtar.tar\n";

        var service = new ArchiveStatusServiceImpl(dataArchiveConfiguration, processRunner);

        try {
            Mockito.when(processRunner.run((String[]) Mockito.any()))
                .thenReturn(new ProcessResult(0, output));

            var result = service.getFileStatus("ff99d9fd-53ef-48f2-8672-a40a2c91f1c6");

            assertEquals(ArchiveStatusService.FileStatus.UNKNOWN, result.get("ff99d9fd-53ef-48f2-8672-a40a2c91f1c6.dfmtar/0000/ff99d9fd-53ef-48f2-8672-a40a2c91f1c6.dfmtar.tar"));
        }
        catch (Exception e) {
            fail(e);
        }
    }
}
