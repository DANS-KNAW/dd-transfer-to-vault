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

import io.dropwizard.hibernate.UnitOfWork;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ttv.client.GmhClient;
import nl.knaw.dans.ttv.core.NbnRegistration;
import nl.knaw.dans.ttv.db.NbnRegistrationDao;

import java.net.URI;
import java.time.OffsetDateTime;

@Slf4j
@RequiredArgsConstructor
public class RegistrationWorker implements Runnable {
    @NonNull
    private final GmhClient gmhClient;
    @NonNull
    private final URI locationBaseUrl;
    @NonNull
    private final NbnRegistrationDao nbnRegistrationDao;

    private final long registrationInterval;
    private boolean retryFailed = false;

    private boolean running;

    @Override
    public void run() {
        try {
            running = true;
            log.info("Starting RegistrationWorker");
            while (running) {
                getAndProcessNextBatch();
                retryFailed = !retryFailed;
                try {
                    log.debug("Sleeping for {} ms", registrationInterval);
                    Thread.sleep(registrationInterval);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } catch (Exception e) {
            log.error("Unexpected error in RegistrationWorker", e);
        }
    }

    @UnitOfWork
    public void getAndProcessNextBatch() {
        log.debug("Getting next batch of {} NBNs to register", retryFailed ? "failed" : "pending");
        var nextBatch = retryFailed ? nbnRegistrationDao.getFailedRegistrations() : nbnRegistrationDao.getPendingRegistrations();
        if (nextBatch.isEmpty()) {
            log.debug("No NBNs to register, switching to {} NBNs", retryFailed ? "pending" : "failed");
            nextBatch = retryFailed ? nbnRegistrationDao.getPendingRegistrations() : nbnRegistrationDao.getFailedRegistrations();
        }
        log.debug("Processing {} NBNs", nextBatch.size());
        nextBatch.forEach(nbnRegistration -> {
            try {
                log.debug("Registering NBN {}", nbnRegistration.getNbn());
                gmhClient.registerNbn(nbnRegistration);
                nbnRegistration.setStatus(NbnRegistration.Status.REGISTERED);
                nbnRegistration.setTimestampMessage("Registration successful");
                nbnRegistration.setTimestamp(OffsetDateTime.now());
                nbnRegistrationDao.save(nbnRegistration);
                log.info("NBN {} registered", nbnRegistration.getNbn());
            }
            catch (Exception e) {
                log.warn("Failed to register NBN {}", nbnRegistration.getNbn(), e);
                nbnRegistration.setStatus(NbnRegistration.Status.FAILED);
                nbnRegistration.setTimestampMessage(e.getMessage());
                nbnRegistration.setTimestamp(OffsetDateTime.now());
                nbnRegistrationDao.save(nbnRegistration);
                log.debug("Rescheduled NBN {} for registration", nbnRegistration.getNbn());
            }
        });
    }

    public void stop() {
        running = false;
    }
}
