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
package nl.knaw.dans.ttv.core;

import io.dropwizard.lifecycle.Managed;
import it.sauronsoftware.cron4j.Scheduler;

import java.util.ArrayList;
import java.util.List;

public class CronCallbackTask implements Managed {

    private final Scheduler scheduler;
    private final List<CronCallback> cronCallbacks = new ArrayList<>();
    private final String schedule;

    public CronCallbackTask(String schedule) {
        this.scheduler = new Scheduler();
        this.schedule = schedule;
    }

    public void addCallback(CronCallback callback) {
        this.cronCallbacks.add(callback);
    }

    private void onCallback() {
        try {
            for (var callback : this.cronCallbacks) {
                callback.onCronTriggered();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void start() throws Exception {
        scheduler.schedule(schedule, this::onCallback);
        // Starts the scheduler.
        scheduler.start();
    }

    @Override
    public void stop() throws Exception {
        scheduler.stop();
    }
}
