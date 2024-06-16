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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;
import nl.knaw.dans.convert.jackson.UriAddTrailingSlashConverter;

import javax.validation.constraints.NotNull;
import java.net.URI;

@Data
public class NbnRegistrationConfig {
    @NotNull
    private GmhConfig gmh;

    @NotNull
    @JsonDeserialize(converter = UriAddTrailingSlashConverter.class)
    private URI catalogBaseUrl;

    private long registrationInterval;
}
