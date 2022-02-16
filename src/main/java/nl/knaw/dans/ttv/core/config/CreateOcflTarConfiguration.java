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
package nl.knaw.dans.ttv.core.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import nl.knaw.dans.lib.util.ExecutorServiceFactory;
import nl.knaw.dans.ttv.core.config.converter.StringByteSizeConverter;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

public class CreateOcflTarConfiguration {

    @Valid
    @NotNull
    private Path inbox;

    @Valid
    @NotNull
    private Path workDir;

    @Valid
    @NotNull
    @JsonDeserialize(converter = StringByteSizeConverter.class)
    private long inboxThreshold;

    @Valid
    @NotNull
    @Min(1)
    private long pollingInterval;

    @Valid
    @NotNull
    @Min(1)
    private int maxRetries;

    @Valid
    @NotNull
    private Duration retryInterval;

    @Valid
    @NotNull
    private List<Duration> retrySchedule;

    @Valid
    @NotNull
    private ExecutorServiceFactory taskQueue;

    public Duration getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(Duration retryInterval) {
        this.retryInterval = retryInterval;
    }

    public List<Duration> getRetrySchedule() {
        return retrySchedule;
    }

    public void setRetrySchedule(List<Duration> retrySchedule) {
        this.retrySchedule = retrySchedule;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getPollingInterval() {
        return pollingInterval;
    }

    public void setPollingInterval(long pollingInterval) {
        this.pollingInterval = pollingInterval;
    }

    public Path getInbox() {
        return inbox;
    }

    public void setInbox(Path inbox) {
        this.inbox = inbox;
    }

    public Path getWorkDir() {
        return workDir;
    }

    public void setWorkDir(Path workDir) {
        this.workDir = workDir;
    }

    public long getInboxThreshold() {
        return inboxThreshold;
    }

    public void setInboxThreshold(long inboxThreshold) {
        this.inboxThreshold = inboxThreshold;
    }

    public ExecutorServiceFactory getTaskQueue() {
        return taskQueue;
    }

    public void setTaskQueue(ExecutorServiceFactory taskQueue) {
        this.taskQueue = taskQueue;
    }
}
