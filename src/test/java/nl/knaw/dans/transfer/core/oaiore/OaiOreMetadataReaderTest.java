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
package nl.knaw.dans.transfer.core.oaiore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class OaiOreMetadataReaderTest {

    @Test
    public void testReadMetadata() throws Exception {
        var reader = new OaiOreMetadataReader();
        String json = """
            {
              "dcterms:modified": "2025-05-19",
              "dcterms:creator": "DANS Data Station Archaeology (dev)",
              "@type": "ore:ResourceMap",
              "schema:additionalType": "Dataverse OREMap Format v1.0.0",
              "dvcore:generatedBy": {
                "@type": "schema:SoftwareApplication",
                "schema:name": "Dataverse",
                "schema:version": "6.3 build DANS-DataStation-PATCH-10",
                "schema:url": "https://github.com/iqss/dataverse"
              },
              "@id": "https://dev.archaeology.datastations.nl/api/datasets/export?exporter=OAI_ORE&persistentId=https://doi.org/10.5072/DAR/ZZJH97",
              "ore:describes": {
                "citation:datasetContact": {
                  "citation:datasetContactName": "Admin, Dataverse",
                  "citation:datasetContactAffiliation": "Dataverse.org",
                  "citation:datasetContactEmail": "dataverse@mailinator.com"
                },
                "author": {
                  "citation:authorName": "Admin, Dataverse",
                  "citation:authorAffiliation": "Dataverse.org"
                },
                "citation:dsDescription": {
                  "citation:dsDescriptionValue": "Test"
                },
                "dansDataVaultMetadata:dansBagId": "urn:uuid:8f3a31bb-90c2-4c34-a101-3e5028845596",
                "dansRelationMetadata:dansAudience": {
                  "@id": "https://www.narcis.nl/classification/D37000",
                  "termName": [
                    {
                      "lang": "nl",
                      "value": "Archeologie"
                    },
                    {
                      "lang": "en",
                      "value": "Archaeology"
                    }
                  ],
                  "vocabularyUri": "https://www.narcis.nl/classification/"
                },
                "dansDataVaultMetadata:dansNbn": "urn:nbn:nl:ui:13-307ab602-abfa-44c0-b35b-fd75e97105a4",
                "dateOfDeposit": "2025-04-08",
                "citation:depositor": "Admin, Dataverse",
                "dansRights:dansRightsHolder": "DANS",
                "dansRights:dansMetadataLanguage": "Abkhaz",
                "citation:subtitle": "y",
                "subject": [
                  "Agricultural Sciences",
                  "Arts and Humanities"
                ],
                "dansRights:dansPersonalDataPresent": "No",
                "dansDataVaultMetadata:dansDataversePid": "doi:10.5072/DAR/ZZJH97",
                "dansDataVaultMetadata:dansDataversePidVersion": "2.0",
                "title": "Manual Test",
                "@id": "https://doi.org/10.5072/DAR/ZZJH97",
                "@type": [
                  "ore:Aggregation",
                  "schema:Dataset"
                ],
                "schema:version": "2.0",
                "schema:name": "Manual Test",
                "schema:dateModified": "2025-05-19 16:33:04.309",
                "schema:datePublished": "2025-04-08",
                "schema:creativeWorkStatus": "RELEASED",
                "schema:license": "http://creativecommons.org/publicdomain/zero/1.0",
                "dvcore:fileTermsOfAccess": {
                  "dvcore:fileRequestAccess": true
                },
                "schema:includedInDataCatalog": "DANS Data Station Archaeology (dev)",
                "schema:isPartOf": {
                  "schema:name": "DANS Data Station Archaeology (dev)",
                  "@id": "https://dev.archaeology.datastations.nl/dataverse/root",
                  "schema:description": "<p>This Data Station allows you to deposit and search for data within the field of Archaeology.<BR> If you want to deposit data, please consult <a href=\\"https://dans.knaw.nl/en/selection-policy-dans-data-stations/\\">the selection policy</a> of the DANS Data Stations.</p>"
                },
                "ore:aggregates": [],
                "schema:hasPart": []
              },
              "@context": {
                "author": "http://purl.org/dc/terms/creator",
                "citation": "https://dataverse.org/schema/citation/",
                "content": "@value",
                "dansDataVaultMetadata": "https://schemas.dans.knaw.nl/metadatablock/dansDataVaultMetadata#",
                "dansRelationMetadata": "https://schemas.dans.knaw.nl/metadatablock/dansRelationMetadata#",
                "dansRights": "https://dev.archaeology.datastations.nl/schema/dansRights#",
                "dateOfDeposit": "http://purl.org/dc/terms/dateSubmitted",
                "dcterms": "http://purl.org/dc/terms/",
                "dvcore": "https://dataverse.org/schema/core#",
                "lang": "@language",
                "ore": "http://www.openarchives.org/ore/terms/",
                "schema": "http://schema.org/",
                "scheme": "http://www.w3.org/2004/02/skos/core#inScheme",
                "subject": "http://purl.org/dc/terms/subject",
                "termName": "https://schema.org/name",
                "title": "http://purl.org/dc/terms/title",
                "value": "@value",
                "vocabularyName": "https://dataverse.org/schema/vocabularyName",
                "vocabularyUri": "https://dataverse.org/schema/vocabularyUri"
              }
            }
            """;
        var attributes = reader.readMetadata(json);

        assertThat(attributes.getMetadata()).isEqualTo(json);
        assertThat(attributes.getBagId()).isEqualTo("urn:uuid:8f3a31bb-90c2-4c34-a101-3e5028845596");
        assertThat(attributes.getNbn()).isEqualTo("urn:nbn:nl:ui:13-307ab602-abfa-44c0-b35b-fd75e97105a4");
        assertThat(attributes.getDataversePid()).isEqualTo("doi:10.5072/DAR/ZZJH97");
        assertThat(attributes.getDataversePidVersion()).isEqualTo("2.0");
        assertThat(attributes.getTitle()).isEqualTo("Manual Test");
        assertThat(attributes.getExporter()).isEqualTo("Dataverse");
        assertThat(attributes.getExporterVersion()).isEqualTo("6.3 build DANS-DataStation-PATCH-10");
    }


}
