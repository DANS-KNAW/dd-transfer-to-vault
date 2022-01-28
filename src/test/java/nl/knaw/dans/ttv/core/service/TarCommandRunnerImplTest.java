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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class TarCommandRunnerImplTest {

    private ProcessRunner processRunner;

    @BeforeEach
    void setUp() {
        processRunner = Mockito.mock(ProcessRunner.class);
    }

    @Test
    void tarDirectory() {
        var runner = new TarCommandRunnerImpl(processRunner);

        try {
            runner.tarDirectory(Path.of("some/path/1"), "user@account.com:path/1");
            Mockito.verify(processRunner).run(new String[] {
                "dmftar",
                "-c",
                "-f",
                "user@account.com:path/1",
                "some/path/1"
            });
        }
        catch (IOException | InterruptedException e) {
            fail(e);
        }
    }

    @Test
    void tarDirectoryWithNullArguments() {
        var runner = new TarCommandRunnerImpl(processRunner);

        assertThrows(NullPointerException.class, () -> {
            runner.tarDirectory(null, "user@account.com:path/1");
        });

        assertThrows(NullPointerException.class, () -> {
            runner.tarDirectory(Path.of("a"), null);
        });
    }
}
