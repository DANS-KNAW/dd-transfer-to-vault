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
package nl.knaw.dans.transfer.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import lombok.Data;
import nl.knaw.dans.transfer.core.TransferItem;

import java.io.IOException;
import java.util.Optional;

@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.EXISTING_PROPERTY, property = "name", visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(name = "dataset-version", value = DatasetVersionCustomPropertyConfig.class),
    @JsonSubTypes.Type(name = "packaging-format", value = FixedValueCustomPropertyConfig.class),
    @JsonSubTypes.Type(name = "deaccessioned", value = DeaccessionedCustomPropertyConfig.class)
})
public abstract class CustomPropertyConfig {
    private String name;
    public abstract Optional<Object> getValue(TransferItem transferItem) throws IOException;
}
