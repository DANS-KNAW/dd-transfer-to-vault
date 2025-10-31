/*
 * Copyright (C) 2025 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.transfer.core;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.time.OffsetDateTime;
import java.util.List;

@Data
@Builder
public class DveMetadata {
    private OffsetDateTime creationTime;

    private String contactName;
    private String contactEmail;

    private String dataversePid;
    private String dataversePidVersion;
    private String title;
    private String bagId;
    private String nbn;
    @ToString.Exclude
    private String metadata;
    private String otherId;
    private String otherIdVersion;
    private String swordToken;
    private String dataSupplier;
    private String datastation;
    private String exporter;
    private String exporterVersion;
    private List<DataFileMetadata> dataFileAttributes;
}
