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
package nl.knaw.dans.ttv.core.domain;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.Value;

@Data
@Builder
public class FileContentAttributes {
    private String dataversePid;
    private String dataversePidVersion;
    private String bagId;
    private String nbn;
    @ToString.Exclude
    private String metadata;
    @ToString.Exclude
    private String filePidToLocalPath;
    private String otherId;
    private String otherIdVersion;
    private String swordToken;
    private String dataSupplier;
    private String datastation;
}
