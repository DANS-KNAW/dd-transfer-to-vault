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

import nl.knaw.dans.lib.util.healthcheck.DependenciesReadyCheck;
import nl.knaw.dans.transfer.TestDirFixture;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class CollectDveTaskTest extends TestDirFixture {
    private final FileService fileService = new FileServiceImpl();
    private final DependenciesReadyCheck readyCheck = Mockito.mock(DependenciesReadyCheck.class);

    @Test
    public void should_move_dve_to_target_dir() throws Exception {
        // Given
        var nbn = "urn:nbn:nl:ui:13-307ab602-abfa-44c0-b35b-fd75e97105a4";
        var inbox = testDir.resolve("inbox");
        Files.createDirectories(inbox);
        var dest = testDir.resolve("dest");
        Files.createDirectories(dest);
        var failed = testDir.resolve("failed");
        Files.createDirectories(failed);

        var dve = inbox.resolve("dve.zip");
        Files.copy(Path.of("src/test/resources/test-dves/doi-10-5072-dar-zzjh97v1.1.zip"), dve);

        var collectDveTask = new CollectDveTask(dve, dest, failed, fileService, readyCheck);

        // When
        collectDveTask.run();

        // Then
        try (var destDirs = Files.list(dest)) {
            assertThat(destDirs
                .filter(Files::isDirectory)
                .map(p -> p.getFileName().toString())
                .anyMatch(name -> name.startsWith(nbn + "-")))
                .isTrue();
        }

    }

    @Test
    public void should_fail_nonzip_dve_candidate() throws Exception {
        // Given
        var inbox = testDir.resolve("inbox");
        Files.createDirectories(inbox);
        var dest = testDir.resolve("dest");
        Files.createDirectories(dest);
        var failed = testDir.resolve("failed");
        Files.createDirectories(failed);

        var nonZipFile = inbox.resolve("nonzip.txt");
        Files.writeString(nonZipFile, "This is not a zip file");

        var collectDveTask = new CollectDveTask(nonZipFile, dest, inbox.resolve("failed"), fileService, readyCheck);

        // When
        collectDveTask.run();

        // Then
        assertThat(inbox.resolve("failed/nonzip.txt")).exists();
        assertThat(inbox.resolve("failed/nonzip.txt-error.log")).exists();
        assertThat(inbox.resolve("failed/nonzip.txt-error.log")).content().contains("not a ZIP file:");
    }

    @Test
    public void should_fail_dve_without_oai_ore_file() throws Exception {
        // Given
        var inbox = testDir.resolve("inbox");
        Files.createDirectories(inbox);
        var dest = testDir.resolve("dest");
        Files.createDirectories(dest);

        var dve = inbox.resolve("dve.zip");
        Files.copy(Path.of("src/test/resources/test-dves/doi-10-5072-dar-zzjh97v1.1-no-oai-ore.zip"), dve);

        var collectDveTask = new CollectDveTask(dve, dest, inbox.resolve("failed"), fileService, readyCheck);

        // When
        collectDveTask.run();

        // Then
        assertThat(inbox.resolve("failed/dve.zip")).exists();
        assertThat(inbox.resolve("failed/dve.zip-error.log")).exists();
        assertThat(inbox.resolve("failed/dve.zip-error.log")).content().contains("No metadata file found in DVE");
    }

    @Test
    public void should_fail_dve_with_oai_ore_but_without_nbn() throws Exception {
        // Given
        var inbox = testDir.resolve("inbox");
        Files.createDirectories(inbox);
        var dest = testDir.resolve("dest");
        Files.createDirectories(dest);

        var dve = inbox.resolve("dve.zip");
        Files.copy(Path.of("src/test/resources/test-dves/doi-10-5072-dar-zzjh97v1.1-no-nbn.zip"), dve);

        var collectDveTask = new CollectDveTask(dve, dest, inbox.resolve("failed"), fileService, readyCheck);

        // When
        collectDveTask.run();

        // Then
        assertThat(inbox.resolve("failed/dve.zip")).exists();
        assertThat(inbox.resolve("failed/dve.zip-error.log")).exists();
        assertThat(inbox.resolve("failed/dve.zip-error.log")).content().contains("No NBN found in DVE");
    }

}
