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
package nl.knaw.dans.ttv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.knaw.dans.lib.util.UrnUuid;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.ttv.core.domain.FileContentAttributes;
import nl.knaw.dans.vaultcatalog.client.api.DatasetDto;
import nl.knaw.dans.vaultcatalog.client.api.VersionExportDto;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;

import java.net.URI;
import java.util.Map;
import java.util.UUID;


@Mapper
public interface Conversions {
    ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Mapping(constant = "false", target = "skeletonRecord")
    @Mapping(source = "creationTime", target = "createdTimestamp")
    VersionExportDto mapTransferItemToVersionExportDto(TransferItem transferItem);

    DatasetDto mapTransferItemToDatasetDto(TransferItem transferItem);
    
    @Mapping(constant = "false", target = "skeletonRecord")
    VersionExportDto updateVersionExportDtoFromTransferItem(TransferItem transferItem, @MappingTarget VersionExportDto versionExportDto);

    // TODO: move reusable fields and methods to a separate interface in dans-java-utils: DefaultConversions
    default UUID stringToUuid(String value) {
        if (value == null) {
            return null;
        }
        return UrnUuid.fromString(value).getUuid();
    }

    default String uuidToString(UUID value) {
        if (value == null) {
            return null;
        }
        var uri = URI.create("urn:uuid:" + value);
        return uri.toString();
    }

    default String objectToString(Object value) {
        if (value == null) {
            return null;
        }

        return value.toString();
    }

    default String mapToJsonString(Map<String, Object> value) throws JsonProcessingException {
        if (value == null) {
            return null;
        }

        return OBJECT_MAPPER.writeValueAsString(value);
    }

    default Map<String, Object> jsonStringToMap(String value) {
        if (value == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.readValue(value, new TypeReference<>() {

            });
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(String.format("Unable to parse JSON: %s", value), e);
        }
    }
}
