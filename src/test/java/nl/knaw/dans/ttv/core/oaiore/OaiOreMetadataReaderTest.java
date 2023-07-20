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
package nl.knaw.dans.ttv.core.oaiore;

import nl.knaw.dans.ttv.core.domain.FileContentAttributes;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

class OaiOreMetadataReaderTest {

    @Test
    void readMetadata_should_parse_dataverse_file() throws Exception {
        var jsonData = Files.readString(
            Path.of(Objects.requireNonNull(getClass().getResource("/json/dataverse.jsonld")).getPath()),
            StandardCharsets.UTF_8
        );

        var result = new OaiOreMetadataReader().readMetadata(jsonData);

        var expected = FileContentAttributes.builder()
            .nbn("urn:nbn:nl:ui:13-ar2-u8v")
            .dataversePid("doi:10.5072/DAR/A7AXZP")
            .dataversePidVersion("1.0")
            .bagId("urn:uuid:e1293f37-a334-4559-a02f-4eaa314e57fd")
            .otherId("doi:10-")
            .otherIdVersion("5.3")
            .dataSupplier("user001")
            .swordToken("sword:123e4567-e89b-12d3-a456-556642440000")
            .datastation("DANS Data Station Archaeology (dev)")
            .build();

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void readMetadata_should_parse_vaas_file() throws Exception {
        var jsonData = Files.readString(
            Path.of(Objects.requireNonNull(getClass().getResource("/json/vault-ingest-flow.jsonld")).getPath()),
            StandardCharsets.UTF_8
        );

        var expected = FileContentAttributes.builder()
            .nbn("urn:nbn:nl:ui:13-d235a58d-84d5-4062-b008-40fec7ebcd83")
            .dataversePid(null)
            .dataversePidVersion(null)
            .bagId("urn:uuid:0b9bb5ee-3187-4387-bb39-2c09536c79f7")
            .otherId("1234; DCTERMS_ID001; DCTERMS_ID002; DCTERMS_ID003")
            .otherIdVersion(null)
            .dataSupplier("user001")
            .swordToken("sword:0b9bb5ee-3187-4387-bb39-2c09536c79f7")
            .datastation("DANS Vault Service")
            .build();

        var result = new OaiOreMetadataReader().readMetadata(jsonData);

        assertThat(result).isEqualTo(expected);
    }
}