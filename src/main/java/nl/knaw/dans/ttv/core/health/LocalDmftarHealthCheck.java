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
import nl.knaw.dans.ttv.core.service.ProcessRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LocalDmftarHealthCheck extends HealthCheck {
    private static final Logger log = LoggerFactory.getLogger(LocalDmftarHealthCheck.class);

    private final DdTransferToVaultConfiguration configuration;
    private final ProcessRunner processRunner;

    public LocalDmftarHealthCheck(DdTransferToVaultConfiguration configuration, ProcessRunner processRunner) {
        this.configuration = configuration;
        this.processRunner = processRunner;
    }

    @Override
    protected Result check() throws Exception {
        log.debug("Checking if dmftar is available");
        var healthy = true;

        try {
            var result = processRunner.run(new String[] { "dmftar", "--version" });

            if (result.getStatusCode() != 0) {
                log.error("Return code for command 'dmftar --version' is {}, expected 0", result.getStatusCode());
                healthy = false;
            }
            else {
                // do the version check
                // note that on macos, the version is 2.0, but on the servers it is 2.2
                // there seems to be no functional difference
                try {
                    var version = result.getStdout().split("\\n")[0].split(" ")[2];
                    var expected = configuration.getCreateOcflTar().getDmftarVersion().getLocal();

                    if (version.equals(expected)) {
                        log.debug("Local dmftar version is correct, installed version is {}, expected {}",
                            version, expected);
                    }
                    else {
                        log.error("Local dmftar version is incorrect, installed version is {}, expected {}",
                            version, expected);
                        healthy = false;
                    }
                }
                catch (RuntimeException e) {
                    log.error("Unable to parse dmftar output", e);
                    healthy = false;
                }
            }
        }
        catch (IOException | InterruptedException e) {
            log.error("Unable to run dmftar --version", e);
            healthy = false;
        }

        if (healthy) {
            return Result.healthy();
        }
        else {
            return Result.unhealthy("Command dmftar is not working as expected, or the version is incorrect");
        }
    }
}
