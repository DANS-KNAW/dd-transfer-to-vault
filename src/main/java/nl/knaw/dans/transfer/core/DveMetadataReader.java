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

import lombok.AllArgsConstructor;
import nl.knaw.dans.transfer.core.oaiore.OaiOreMetadataReader;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.Properties;

@AllArgsConstructor
public class DveMetadataReader {
    private final FileService fileService;
    private final OaiOreMetadataReader oaiOreMetadataReader;
    private final DataFileMetadataReader dataFileMetadataReader;

    public DveMetadata readDveMetadata(Path path) {

        try {
            var datasetVersionExport = fileService.openZipFile(path);
            var metadataInputstream = fileService.getEntryUnderBaseFolder(datasetVersionExport, Path.of("metadata/oai-ore.jsonld"));
            var oaiOre = IOUtils.toString(metadataInputstream, StandardCharsets.UTF_8);
            var dveMetadata = oaiOreMetadataReader.readMetadata(oaiOre);
            var propertiesPath = path.getParent().resolve(path.getFileName() + ".properties");
            String creationTimeValue = null;
            if (Files.exists(propertiesPath)) {
                var props = new Properties();
                try (var reader = Files.newBufferedReader(propertiesPath)) {
                    props.load(reader);
                    creationTimeValue = props.getProperty("creationTime");
                    dveMetadata.setCreationTime(OffsetDateTime.parse(creationTimeValue));
                }
            }
            else {
                throw new IllegalStateException("Missing properties file: " + propertiesPath);
            }

            var dataFileAttributes = dataFileMetadataReader.readDataFileAttributes(path);
            dveMetadata.setDataFileAttributes(dataFileAttributes);

            return dveMetadata;
        }
        catch (IOException e) {
            throw new RuntimeException("unable to read metadata from file", e);
        }
    }
}
