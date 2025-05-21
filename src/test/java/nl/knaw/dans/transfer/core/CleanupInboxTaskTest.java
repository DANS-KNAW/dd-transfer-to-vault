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
import org.junit.jupiter.api.Test;

import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

public class CleanupInboxTaskTest extends TestDirFixture {

    @Test
    public void testRemoveEmptySubdirs() throws Exception {
        // Given
        var subdir1 = testDir.resolve("nonEmptySubdir1");
        var subdir2 = testDir.resolve("nonEmptySubdir2");
        var emptySubdir = testDir.resolve("emptySubdir");
        Files.createDirectories(subdir1);
        Files.createFile(subdir1.resolve("file.txt"));
        Files.createDirectories(subdir2);
        Files.createFile(subdir2.resolve("file.txt"));
        Files.createDirectories(emptySubdir);
        assertThat(subdir1).exists();
        assertThat(subdir2).exists();
        assertThat(emptySubdir).exists();

        // When
        new RemoveEmptyTargetDirsTask(testDir).run();

        // Then
        assertThat(subdir1).exists();
        assertThat(subdir2).exists();
        assertThat(emptySubdir).doesNotExist();
    }

}
