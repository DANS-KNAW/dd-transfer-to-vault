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
import lombok.Data;
import nl.knaw.dans.lib.util.ExecutorServiceFactory;
import nl.knaw.dans.ttv.core.config.converter.StringByteSizeConverter;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

@Data
public class CreateOcflTarConfiguration {
    @Valid
    @NotNull
    private Path inbox;

    @Valid
    @NotNull
    private Path workDir;
    @Valid
    @NotNull
    private DmfTarVersion dmftarVersion;
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

    @Data
    public static class DmfTarVersion {
        @NotNull
        private String local;
        @NotNull
        private String remote;

        public DmfTarVersion() {

        }

        public DmfTarVersion(String local, String remote) {
            this.local = local;
            this.remote = remote;
        }
    }
}
