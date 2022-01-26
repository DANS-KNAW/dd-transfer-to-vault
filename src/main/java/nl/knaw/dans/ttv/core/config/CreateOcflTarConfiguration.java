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

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class CreateOcflTarConfiguration {

    @Valid
    @NotNull
    private String inbox;

    @Valid
    @NotNull
    private String workDir;

    @Valid
    @NotNull
    @JsonDeserialize(converter = StringByteSizeConverter.class)
    private long inboxThreshold;

    @Valid
    @NotNull
    private String tarCommand;

    @Valid
    @NotNull
    private ExecutorServiceFactory taskQueue;

    public String getInbox() {
        return inbox;
    }

    public void setInbox(String inbox) {
        this.inbox = inbox;
    }

    public String getWorkDir() {
        return workDir;
    }

    public void setWorkDir(String workDir) {
        this.workDir = workDir;
    }

    public long getInboxThreshold() {
        return inboxThreshold;
    }

    public void setInboxThreshold(long inboxThreshold) {
        this.inboxThreshold = inboxThreshold;
    }

    public String getTarCommand() {
        return tarCommand;
    }

    public void setTarCommand(String tarCommand) {
        this.tarCommand = tarCommand;
    }

    public ExecutorServiceFactory getTaskQueue() {
        return taskQueue;
    }

    public void setTaskQueue(ExecutorServiceFactory taskQueue) {
        this.taskQueue = taskQueue;
    }
}
