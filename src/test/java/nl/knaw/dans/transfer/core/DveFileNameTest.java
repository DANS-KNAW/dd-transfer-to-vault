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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DveFileNameTest {

    @Test
    public void should_get_creationTime_and_version_from_name() {
        // Given
        long expectedCreationDateTime = 1625078400000L; // corresponds to 2021-06-30T00:00:00Z
        var dveFileName = new DveFileName(Path.of(String.format("some_dataset_%s_v3.zip", expectedCreationDateTime)));

        // Then
        assertThat(dveFileName.getCreationDateTime().toInstant().toEpochMilli()).isEqualTo(expectedCreationDateTime);
        assertThat(dveFileName.getOcflObjectVersion()).isEqualTo(3);
    }

    @Test
    public void should_get_null_creationTime_when_missing_in_name() {
        // Given
        var dveFileName = new DveFileName(Path.of("some_dataset_v10.zip"));

        // Then
        assertThat(dveFileName.getCreationDateTime()).isNull();
        assertThat(dveFileName.getOcflObjectVersion()).isEqualTo(10);
    }

    @Test
    public void should_get_null_version_when_missing_in_name() {
        // Given
        long expectedCreationDateTime = 1625078400000L; // corresponds to 2021-06-30T00:00:00Z
        var dveFileName = new DveFileName(Path.of(String.format("some_dataset_%s.zip", expectedCreationDateTime)));

        // Then
        assertThat(dveFileName.getCreationDateTime().toInstant().toEpochMilli()).isEqualTo(expectedCreationDateTime);
        assertThat(dveFileName.getOcflObjectVersion()).isNull();
    }


    @Test
    public void should_throw_exception_on_invalid_name() {
        assertThatThrownBy(() -> new DveFileName(Path.of("some_dataset_v10.txt")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("DVE filename does not match expected pattern");
    }
}
