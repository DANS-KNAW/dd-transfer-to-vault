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
import nl.knaw.dans.ttv.DdTransferToVaultConfiguration;
import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RemoteDmftarHealthCheck extends HealthCheck {
    private static final Logger log = LoggerFactory.getLogger(RemoteDmftarHealthCheck.class);

    private final DdTransferToVaultConfiguration configuration;
    private final TarCommandRunner tarCommandRunner;

    public RemoteDmftarHealthCheck(DdTransferToVaultConfiguration configuration, TarCommandRunner tarCommandRunner) {
        this.configuration = configuration;
        this.tarCommandRunner = tarCommandRunner;
    }

    @Override
    protected Result check() throws Exception {
        log.debug("Checking if dmftar is available remotely");
        var healthy = true;
        var reason = "";

        try {
            var result = tarCommandRunner.getDmftarVersion();

            if (result.getStatusCode() != 0) {
                log.error("Return code for command 'dmftar --version' is {}, expected 0", result.getStatusCode());
                healthy = false;
                reason = "Return code for command 'dmftar --version' is " + result.getStatusCode() + ", expected 0";
            }
            else {
                // do the version check
                // note that on macos, the version is 2.0, but on the servers it is 2.2
                // there seems to be no functional difference
                try {
                    var version = result.getStdout().split("\\n")[0].split(" ")[2];
                    var expected = configuration.getCreateOcflTar().getDmftarVersion().getRemote();

                    if (version.equals(expected)) {
                        log.debug("Remote dmftar version is correct, installed version is {}, expected {}",
                            version, expected);
                    }
                    else {
                        log.error("Remote dmftar version is incorrect, installed version is {}, expected {}",
                            version, expected);
                        healthy = false;
                        reason = "Remote dmftar version is incorrect, installed version is " + version + ", expected " + expected;
                    }
                }
                catch (RuntimeException e) {
                    log.error("Unable to parse dmftar output", e);
                    healthy = false;
                    reason = "Unable to parse dmftar output";
                }
            }
        }
        catch (IOException | InterruptedException e) {
            log.error("Unable to run dmftar --version", e);
            healthy = false;
            reason = "Unable to run dmftar --version";
        }

        if (healthy) {
            return Result.healthy();
        }
        else {
            return Result.unhealthy("Command dmftar is not working as expected: " + reason);
        }
    }
}
