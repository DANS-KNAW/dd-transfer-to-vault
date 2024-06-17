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
import io.dropwizard.lifecycle.Managed;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ttv.core.NbnRegistration;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.ttv.db.NbnRegistrationDao;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@RequiredArgsConstructor
public class NbnRegistrationServiceImpl implements NbnRegistrationService {
    @NonNull
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    @NonNull
    private final RegistrationWorker registrationWorker;
    @NonNull
    private final NbnRegistrationDao nbnRegistrationDao;
    @NonNull
    private final URI locationBaseUrl;

    private boolean started;

    @Override
    public synchronized void start() {
        log.info("Starting NbnRegistrationService");
        if (started) {
            log.warn("NbnRegistrationService already started. Ignoring start request.");
            return;
        }

        executorService.submit(registrationWorker);
        started = true;
    }

    @Override
    public void stop() {
        registrationWorker.stop();
        executorService.shutdown();
    }

    @UnitOfWork
    @Override
    public void scheduleNbnRegistration(TransferItem transferItem) {
        NbnRegistration nbnRegistration = NbnRegistration.builder()
            .nbn(transferItem.getNbn())
            .location(locationBaseUrl.resolve(URLEncoder.encode(transferItem.getNbn(), StandardCharsets.UTF_8)))
            .build();
        nbnRegistrationDao.save(nbnRegistration);
    }
}
