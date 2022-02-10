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

class ArchiveMetadataServiceImplTest {

    ProcessRunner processRunner;
    DataArchiveConfiguration dataArchiveConfiguration = new DataArchiveConfiguration("username", "hostname", "");

    @BeforeEach
    void setUp() {
        this.processRunner = Mockito.mock(ProcessRunner.class);
    }

    @Test
    void testChecksum() throws IOException, InterruptedException {
        var output = "dans-vault/0f10d4c8-56a1-46bb-b081-caf34a8d8dc5.dmftar/0000/0f10d4c8-56a1-46bb-b081-caf34a8d8dc5.dmftar.tar.chksum ::: md5 89f2b08d1fd59c2e1e1aed58f7578fb8 0f10d4c8-56a1-46bb-b081-caf34a8d8dc5.dmftar.tar\n";
        var service = new ArchiveMetadataServiceImpl(dataArchiveConfiguration, processRunner);

        Mockito.when(processRunner.run((String[]) Mockito.any()))
            .thenReturn(new ProcessResult(0, output));

        var result = service.getArchiveMetadata("ff99d9fd-53ef-48f2-8672-a40a2c91f1c6");

        assertEquals(1, result.getParts().size());
        assertEquals("0000", result.getParts().get(0).getIdentifier());
        assertEquals("md5", result.getParts().get(0).getChecksumAlgorithm());
        assertEquals("89f2b08d1fd59c2e1e1aed58f7578fb8", result.getParts().get(0).getChecksum());
    }

    @Test
    void testMultipleFiles() throws IOException, InterruptedException {
        var output = "dans-vault/0f10d4c8-56a1-46bb-b081-caf34a8d8dc5.dmftar/0000/0f10d4c8-56a1-46bb-b081-caf34a8d8dc5.dmftar.tar.chksum ::: md5 89f2b08d1fd59c2e1e1aed58f7578fb8 0f10d4c8-56a1-46bb-b081-caf34a8d8dc5.dmftar.tar\n"
            + "dans-vault/0f10d4c8-56a1-46bb-b081-caf34a8d8dc5.dmftar/0001/0f10d4c8-56a1-46bb-b081-caf34a8d8dc5.dmftar.tar.chksum ::: blake2 abc 0f10d4c8-56a1-46bb-b081-caf34a8d8dc5.dmftar.tar\n";
        var service = new ArchiveMetadataServiceImpl(dataArchiveConfiguration, processRunner);

        Mockito.when(processRunner.run((String[]) Mockito.any()))
            .thenReturn(new ProcessResult(0, output));

        var result = service.getArchiveMetadata("ff99d9fd-53ef-48f2-8672-a40a2c91f1c6");

        assertEquals(2, result.getParts().size());
        assertEquals("0000", result.getParts().get(0).getIdentifier());
        assertEquals("md5", result.getParts().get(0).getChecksumAlgorithm());
        assertEquals("89f2b08d1fd59c2e1e1aed58f7578fb8", result.getParts().get(0).getChecksum());
        assertEquals("0001", result.getParts().get(1).getIdentifier());
        assertEquals("blake2", result.getParts().get(1).getChecksumAlgorithm());
        assertEquals("abc", result.getParts().get(1).getChecksum());
    }

    @Test
    void testCommandError() throws IOException, InterruptedException {
        var output = "dans-vault/0f10d4c8-56a1-46bb-b081-caf34a8d8dc5.dmftar/0000/0f10d4c8-56a1-46bb-b081-caf34a8d8dc5.dmftar.tar.chksum ::: md5 89f2b08d1fd59c2e1e1aed58f7578fb8 0f10d4c8-56a1-46bb-b081-caf34a8d8dc5.dmftar.tar\n";
        var service = new ArchiveMetadataServiceImpl(dataArchiveConfiguration, processRunner);

        Mockito.when(processRunner.run((String[]) Mockito.any()))
            .thenReturn(new ProcessResult(1, output));

        assertThrows(IOException.class, () -> {
            service.getArchiveMetadata("ff99d9fd-53ef-48f2-8672-a40a2c91f1c6");
        });
    }
}
