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
import nl.knaw.dans.ttv.core.config.DdTransferToVaultConfig;
import nl.knaw.dans.ttv.core.config.CreateOcflTarConfig;
import nl.knaw.dans.ttv.core.domain.ProcessResult;
import nl.knaw.dans.ttv.core.service.ProcessRunner;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class LocalDmftarHealthCheckTest {

    @Test
    void testCommandExists() throws Exception {
        var processRunner = Mockito.mock(ProcessRunner.class);
        var config = new DdTransferToVaultConfig();
        config.setCreateOcflTar(new CreateOcflTarConfig());
        config.getCreateOcflTar().setDmftarVersion(new CreateOcflTarConfig.DmfTarVersion("2.0", "2.0"));

        Mockito.when(processRunner.run(Mockito.any()))
            .thenReturn(new ProcessResult(0, "dmftar version 2.0\ncopyright"));

        var result = new LocalDmftarHealthCheck(config, processRunner).check();

        var healtyhStatus = HealthCheck.Result.healthy();
        assertTrue(result.isHealthy());
        assertEquals(healtyhStatus.getMessage(), result.getMessage());
    }

    @Test
    void testCommandDoesNotExists() throws Exception {
        var processRunner = Mockito.mock(ProcessRunner.class);
        var config = new DdTransferToVaultConfig();
        config.setCreateOcflTar(new CreateOcflTarConfig());
        config.getCreateOcflTar().setDmftarVersion(new CreateOcflTarConfig.DmfTarVersion("2.0", "2.0"));

        Mockito.when(processRunner.run(Mockito.any()))
            .thenReturn(new ProcessResult(255, "dmftar version 2.0\ncopyright"));

        var result = new LocalDmftarHealthCheck(config, processRunner).check();

        assertFalse(result.isHealthy());
    }

    @Test
    void testCommandHasWrongVersion() throws Exception {
        var processRunner = Mockito.mock(ProcessRunner.class);
        var config = new DdTransferToVaultConfig();
        config.setCreateOcflTar(new CreateOcflTarConfig());
        config.getCreateOcflTar().setDmftarVersion(new CreateOcflTarConfig.DmfTarVersion("3.0", "3.0"));

        Mockito.when(processRunner.run(Mockito.any()))
            .thenReturn(new ProcessResult(0, "dmftar version 2.0\ncopyright"));

        var result = new LocalDmftarHealthCheck(config, processRunner).check();

        assertFalse(result.isHealthy());
    }
}

