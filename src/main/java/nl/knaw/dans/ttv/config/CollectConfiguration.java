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
package nl.knaw.dans.ttv.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import nl.knaw.dans.lib.util.ExecutorServiceFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

public class CollectConfiguration {
    @Valid
    @NotNull
    @JsonProperty("inboxes")
    private List<Map<String, String>> inboxes;

    @Valid
    @NotNull
    @JsonProperty("taskQueue")
    private ExecutorServiceFactory taskQueue;

    public List<Map<String, String>> getInboxes() {
        return inboxes;
    }

    public void setInboxes(List<Map<String, String>> inboxes) {
        this.inboxes = inboxes;
    }

    public ExecutorServiceFactory getTaskQueue() {
        return taskQueue;
    }

    public void setTaskQueue(ExecutorServiceFactory taskQueue) {
        this.taskQueue = taskQueue;
    }
}
