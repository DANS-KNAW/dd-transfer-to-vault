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
package nl.knaw.dans.ttv.core.health;

import com.codahale.metrics.health.HealthCheck;
import nl.knaw.dans.ttv.DdTransferToVaultConfiguration;
import nl.knaw.dans.ttv.core.config.CreateOcflTarConfiguration;
import nl.knaw.dans.ttv.core.dto.ProcessResult;
import nl.knaw.dans.ttv.core.service.ProcessRunner;
import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class RemoteDmftarHealthCheckTest {

    @Test
    void testCommandExists() throws Exception {
        var tarCommandRunner = Mockito.mock(TarCommandRunner.class);
        var config = new DdTransferToVaultConfiguration();
        config.setCreateOcflTar(new CreateOcflTarConfiguration());
        config.getCreateOcflTar().setDmftarVersion(new CreateOcflTarConfiguration.DmfTarVersion("2.0", "2.2"));

        Mockito.when(tarCommandRunner.getDmftarVersion())
                .thenReturn(new ProcessResult(0, "dmftar version 2.2\n"
                    + "Copyright 2014 SURFsara B.V.\n"
                    + "Copyright 2021 SURF bv"));

        var result = new RemoteDmftarHealthCheck(config, tarCommandRunner).check();

        assertEquals(HealthCheck.Result.healthy(), result);
    }

    @Test
    void testCommandDoesNotExists() throws Exception {
        var tarCommandRunner = Mockito.mock(TarCommandRunner.class);

        var config = new DdTransferToVaultConfiguration();
        config.setCreateOcflTar(new CreateOcflTarConfiguration());
        config.getCreateOcflTar().setDmftarVersion(new CreateOcflTarConfiguration.DmfTarVersion("2.0", "2.2"));

        Mockito.when(tarCommandRunner.getDmftarVersion())
            .thenReturn(new ProcessResult(255, "dmftar version 2.2\n"
                + "Copyright 2014 SURFsara B.V.\n"
                + "Copyright 2021 SURF bv"));

        var result = new RemoteDmftarHealthCheck(config, tarCommandRunner).check();

        assertFalse(result.isHealthy());
    }

    @Test
    void testCommandHasWrongVersion() throws Exception {
        var tarCommandRunner = Mockito.mock(TarCommandRunner.class);
        var config = new DdTransferToVaultConfiguration();
        config.setCreateOcflTar(new CreateOcflTarConfiguration());
        config.getCreateOcflTar().setDmftarVersion(new CreateOcflTarConfiguration.DmfTarVersion("3.0", "3.0"));

        Mockito.when(tarCommandRunner.getDmftarVersion())
            .thenReturn(new ProcessResult(0, "dmftar version 2.2\n"
                + "Copyright 2014 SURFsara B.V.\n"
                + "Copyright 2021 SURF bv"));

        var result = new RemoteDmftarHealthCheck(config, tarCommandRunner).check();

        assertFalse(result.isHealthy());
    }
}

