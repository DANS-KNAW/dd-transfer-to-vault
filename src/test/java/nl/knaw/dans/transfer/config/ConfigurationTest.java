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

import com.fasterxml.jackson.databind.DeserializationFeature;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.validation.Validators;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConfigurationTest {

    private final Path testDir = new File("target/test/" + getClass().getSimpleName()).toPath();
    private YamlConfigurationFactory<DdTransferToVaultConfiguration> factory;

    @BeforeEach
    void setUp() throws IOException {
        FileUtils.deleteQuietly(testDir.toFile());
        Files.createDirectories(testDir);
        final var mapper = Jackson.newObjectMapper().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        factory = new YamlConfigurationFactory<>(DdTransferToVaultConfiguration.class, Validators.newValidator(), mapper, "dw");
    }

    @Test
    public void assembly_dist_cfg_does_not_throw() throws IOException {
        File cfgFile = testDir.resolve("config.yml").toFile();
        Files.copy(Paths.get("src/main/assembly/dist/cfg/config.yml"), cfgFile.toPath());
        var configuration = assertDoesNotThrow(() -> factory.build(FileInputStream::new, cfgFile.toString()));
        
        assertNotNull(configuration.getTransfer().getSendToVault().getCustomProperties());
        assertEquals(2, configuration.getTransfer().getSendToVault().getCustomProperties().size());
        var datasetVersion = configuration.getTransfer().getSendToVault().getCustomProperties().get("dataset-version");
        assertNotNull(datasetVersion);
        assertEquals(DatasetVersionCustomPropertyConfig.class, datasetVersion.getClass());
        assertEquals("dansDataversePidVersion", ((DatasetVersionCustomPropertyConfig) datasetVersion).getSource());
        assertEquals(true, ((DatasetVersionCustomPropertyConfig) datasetVersion).getFailIfMissing());

        var packagingFormat = configuration.getTransfer().getSendToVault().getCustomProperties().get("packaging-format");
        assertNotNull(packagingFormat);
        assertEquals(FixedValueCustomPropertyConfig.class, packagingFormat.getClass());
        assertEquals("DANS RDA BagPack Profile/0.1.0", ((FixedValueCustomPropertyConfig) packagingFormat).getValue());
    }

    @Test
    public void debug_etc_does_not_throw() throws IOException {
        File cfgFile = testDir.resolve("config.yml").toFile();
        Files.copy(Path.of("src/test/resources/debug-etc/config.yml"), cfgFile.toPath());
        assertDoesNotThrow(() -> factory.build(FileInputStream::new, cfgFile.toString()));
    }
}
