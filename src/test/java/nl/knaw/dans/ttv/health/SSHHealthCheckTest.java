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

import nl.knaw.dans.ttv.core.dto.ProcessResult;
import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SSHHealthCheckTest {

    @Test
    void checkSshWorks() throws Exception {
        var tarCommandRunner = Mockito.mock(TarCommandRunner.class);
        Mockito.when(tarCommandRunner.getDmftarVersion())
            .thenReturn(new ProcessResult(0, "version 1"));

        var result = new SSHHealthCheck(tarCommandRunner).check();

        assertTrue(result.isHealthy());
    }

    @Test
    void checkSshBroken() throws Exception {
        var tarCommandRunner = Mockito.mock(TarCommandRunner.class);
        Mockito.when(tarCommandRunner.getDmftarVersion())
            .thenReturn(new ProcessResult(255, "version 1"));

        var result = new SSHHealthCheck(tarCommandRunner).check();

        assertFalse(result.isHealthy());
    }
}
