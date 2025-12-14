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

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DveFileNameTest {

    @Test
    public void should_get_creationTime_and_version_from_name() {
        // Given
        long expectedCreationDateTime = 1625078400000L; // corresponds to 2021-06-30T00:00:00Z
        var dveFileName = new DveFileName(Path.of(String.format("some_dataset_%s_v3.zip", expectedCreationDateTime)));

        // Then
        assertThat(dveFileName.getCreationTime().toInstant().toEpochMilli()).isEqualTo(expectedCreationDateTime);
        assertThat(dveFileName.getOcflObjectVersion()).isEqualTo(3);
    }

    @Test
    public void should_get_null_creationTime_when_missing_in_name() {
        // Given
        var dveFileName = new DveFileName(Path.of("some_dataset_v10.zip"));

        // Then
        assertThat(dveFileName.getCreationTime()).isNull();
        assertThat(dveFileName.getOcflObjectVersion()).isEqualTo(10);
    }

    @Test
    public void should_get_null_version_when_missing_in_name() {
        // Given
        long expectedCreationDateTime = 1625078400000L; // corresponds to 2021-06-30T00:00:00Z
        var dveFileName = new DveFileName(Path.of(String.format("some_dataset_%s.zip", expectedCreationDateTime)));

        // Then
        assertThat(dveFileName.getCreationTime().toInstant().toEpochMilli()).isEqualTo(expectedCreationDateTime);
        assertThat(dveFileName.getOcflObjectVersion()).isNull();
    }

    @Test
    public void should_throw_exception_on_invalid_name() {
        assertThatThrownBy(() -> new DveFileName(Path.of("some_dataset_v10_no_ext")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("DVE filename does not match expected pattern");
    }

    @Test
    public void should_convert_back_to_path() {
        // Given
        long expectedCreationDateTime = 1625078400000L; // corresponds to 2021-06-30T00:00:00Z
        var originalPath = Path.of(String.format("some_dataset_%s_v3.zip", expectedCreationDateTime));
        var dveFileName = new DveFileName(originalPath);

        // When
        var convertedPath = dveFileName.getPath();

        // Then
        assertThat(convertedPath).isEqualTo(originalPath);
    }

    @Test
    public void should_add_missing_creationTime_and_version() {
        // Given
        long expectedCreationDateTime = 1625078400000L; // corresponds to 2021-06-30T00:00:00Z
        var dveFileName = new DveFileName(Path.of("some_dataset.zip"));

        // When
        var updatedDveFileName = dveFileName
            .withCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(expectedCreationDateTime), ZoneOffset.UTC))
            .withOcflObjectVersion(5);

        // Then
        assertThat(updatedDveFileName.getCreationTime().toInstant().toEpochMilli()).isEqualTo(expectedCreationDateTime);
        assertThat(updatedDveFileName.getOcflObjectVersion()).isEqualTo(5);
        assertThat(updatedDveFileName.getPath()).isEqualTo(Path.of(String.format("some_dataset_%s_v5.zip", expectedCreationDateTime)));
    }

    @Test
    public void should_throw_exception_when_adding_existing_creationTime_or_version() {
        // Given
        long expectedCreationDateTime = 1625078400000L; // corresponds to 2021-06-30T00:00:00Z
        var dveFileName = new DveFileName(Path.of(String.format("some_dataset_%s_v3.zip", expectedCreationDateTime)));

        // Then
        assertThatThrownBy(() -> dveFileName.withCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(expectedCreationDateTime + 1000), ZoneOffset.UTC)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Creation time is already set for DVE");

        assertThatThrownBy(() -> dveFileName.withOcflObjectVersion(5))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("OCFL object version is already set for DVE");
    }

    @Test
    public void should_allow_setting_only_creationTime_or_version() {
        // Given
        long expectedCreationDateTime = 1625078400000L; // corresponds to 2021-06-30T00:00:00Z
        var dveFileName = new DveFileName(Path.of("some_dataset.zip"));

        // When
        var withCreationTime = dveFileName.withCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(expectedCreationDateTime), ZoneOffset.UTC));
        var withVersion = dveFileName.withOcflObjectVersion(7);

        // Then
        assertThat(withCreationTime.getCreationTime().toInstant().toEpochMilli()).isEqualTo(expectedCreationDateTime);
        assertThat(withCreationTime.getOcflObjectVersion()).isNull();
        assertThat(withCreationTime.getPath()).isEqualTo(Path.of(String.format("some_dataset_%s.zip", expectedCreationDateTime)));

        assertThat(withVersion.getCreationTime()).isNull();
        assertThat(withVersion.getOcflObjectVersion()).isEqualTo(7);
        assertThat(withVersion.getPath()).isEqualTo(Path.of("some_dataset_v7.zip"));
    }

    @Test
    public void should_allow_updating_creationTime_twice_with_the_same_value() {
        // Given
        long expectedCreationDateTime = 1625078400000L; // corresponds to 2021-06-30T00:00:00Z
        var dveFileName = new DveFileName(Path.of("some_dataset.zip"));

        // When
        var withCreationTime1 = dveFileName.withCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(expectedCreationDateTime), ZoneOffset.UTC));
        var withCreationTime2 = withCreationTime1.withCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(expectedCreationDateTime), ZoneOffset.UTC));

        // Then
        assertThat(withCreationTime2.getCreationTime().toInstant().toEpochMilli()).isEqualTo(expectedCreationDateTime);
        assertThat(withCreationTime2.getOcflObjectVersion()).isNull();
        assertThat(withCreationTime2.getPath()).isEqualTo(Path.of(String.format("some_dataset_%s.zip", expectedCreationDateTime)));
    }

    @Test
    public void should_allow_updating_version_twice_with_the_same_value() {
        // Given
        var dveFileName = new DveFileName(Path.of("some_dataset.zip"));

        // When
        var withVersion1 = dveFileName.withOcflObjectVersion(4);
        var withVersion2 = withVersion1.withOcflObjectVersion(4);

        // Then
        assertThat(withVersion2.getCreationTime()).isNull();
        assertThat(withVersion2.getOcflObjectVersion()).isEqualTo(4);
        assertThat(withVersion2.getPath()).isEqualTo(Path.of("some_dataset_v4.zip"));
    }

    @Test
    public void should_add_index() {
        // Given
        var dveFileName = new DveFileName(Path.of("some_dataset.zip"));

        // When
        var updatedDveFileName = dveFileName.withIndex(1);

        // Then
        assertThat(updatedDveFileName.getIndex()).isEqualTo(1);
        assertThat(updatedDveFileName.getPath()).isEqualTo(Path.of("some_dataset-1.zip"));
    }
}
