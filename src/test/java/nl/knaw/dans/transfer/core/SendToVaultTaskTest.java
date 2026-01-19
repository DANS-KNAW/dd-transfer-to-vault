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

import nl.knaw.dans.transfer.TestDirFixture;
import nl.knaw.dans.transfer.client.DataVaultClient;
import nl.knaw.dans.transfer.config.CustomPropertyConfig;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class SendToVaultTaskTest extends TestDirFixture {

    @Test
    public void createVersionInfoProperties_should_handle_special_characters_and_whitespace() throws Exception {
        // Given
        Path dve = testDir.resolve("test.zip");
        Path currentBatchWorkDir = testDir.resolve("batch");
        Path dataVaultBatchRoot = testDir.resolve("vault");
        Path outboxProcessed = testDir.resolve("processed");
        Path outboxFailed = testDir.resolve("failed");
        DataVaultClient dataVaultClient = Mockito.mock(DataVaultClient.class);
        FileService fileService = new FileServiceImpl();
        String defaultMessage = "Default message with\nnewline";
        Map<String, CustomPropertyConfig> customProperties = new HashMap<>();

        SendToVaultTask task = new SendToVaultTask(
            dve, currentBatchWorkDir, dataVaultBatchRoot, null, outboxProcessed, outboxFailed,
            dataVaultClient, defaultMessage, customProperties, fileService
        );

        Path versionDirectory = testDir.resolve("v1");
        Files.createDirectories(versionDirectory);
        
        String user = "  John Doe \n  ";
        String email = "\tjohn@example.com\r\n";
        String message = "Message with = and : and \n newline";

        // When
        // Using reflection to call private method for testing
        var method = SendToVaultTask.class.getDeclaredMethod("createVersionInfoProperties", Path.class, String.class, String.class, String.class);
        method.setAccessible(true);
        method.invoke(task, versionDirectory, user, email, message);

        // Then
        Path propertiesFile = testDir.resolve("v1.properties");
        assertThat(propertiesFile).exists();

        Properties props = new Properties();
        try (var is = Files.newInputStream(propertiesFile)) {
            props.load(is);
        }

        assertThat(props.getProperty("user.name")).isEqualTo("John Doe");
        assertThat(props.getProperty("user.email")).isEqualTo("john@example.com");
        assertThat(props.getProperty("message")).isEqualTo(message);
    }
}
