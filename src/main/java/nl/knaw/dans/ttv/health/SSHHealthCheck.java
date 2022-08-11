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
import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SSHHealthCheck extends HealthCheck {
    private static final Logger log = LoggerFactory.getLogger(SSHHealthCheck.class);

    private final TarCommandRunner tarCommandRunner;

    public SSHHealthCheck(TarCommandRunner tarCommandRunner) {
        this.tarCommandRunner = tarCommandRunner;
    }

    @Override
    protected Result check() throws Exception {
        var healthy = true;

        // check ssh access
        log.debug("Checking if SSH connection to archive server is configured correctly");

        try {
            var result = tarCommandRunner.getDmftarVersion();

            if (result.getStatusCode() == 0) {
                log.debug("Successfully connected to remote server and executed command, SSH is properly configured");
            }
            else {
                log.error("Not possible to execute remote SSH command, possibly because of misconfigured ssh access");
                healthy = false;
            }
        }
        catch (IOException | InterruptedException e) {
            log.error("Unable to run remote command", e);
            healthy = false;
        }

        if (healthy) {
            return Result.healthy();
        }
        else {
            return Result.unhealthy("SSH access to remote archive was unsuccessful");
        }
    }
}
