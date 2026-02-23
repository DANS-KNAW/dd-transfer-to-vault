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

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class FileServiceTest {

    @Test
    void isSameFileSystem_should_return_false_when_FileStores_different() {
        var fileService = new FileServiceImpl();
        var path1 = Path.of("/a");
        var path2 = Path.of("/b");
        var fileStore1 = mock(FileStore.class);
        var fileStore2 = mock(FileStore.class);

        try (var filesMock = mockStatic(Files.class)) {
            filesMock.when(() -> Files.getFileStore(path1)).thenReturn(fileStore1);
            filesMock.when(() -> Files.getFileStore(path2)).thenReturn(fileStore2);

            var result = fileService.isSameFileSystem(List.of(path1, path2));
            assertThat(result).isFalse();
        }
    }

    @Test
    void isSameFileSystem_should_return_true_when_FileStores_the_same() {
        var fileService = new FileServiceImpl();
        var path1 = Path.of("/a");
        var path2 = Path.of("/b");
        var fileStore = mock(FileStore.class);

        try (var filesMock = mockStatic(Files.class)) {
            filesMock.when(() -> Files.getFileStore(path1)).thenReturn(fileStore);
            filesMock.when(() -> Files.getFileStore(path2)).thenReturn(fileStore);

            var result = fileService.isSameFileSystem(List.of(path1, path2));
            assertThat(result).isTrue();
        }
    }

    @Test
    void canReadFrom_should_return_false_if_path_does_not_exist() {
        var fileService = new FileServiceImpl();
        var path = Path.of("does-not-exist");

        try (var filesMock = mockStatic(Files.class)) {
            filesMock.when(() -> Files.exists(path)).thenReturn(false);

            var result = fileService.canReadFrom(path);
            assertThat(result).isFalse();
        }
    }

    @Test
    void canReadFrom_should_return_false_if_path_is_not_a_directory() {
        var fileService = new FileServiceImpl();
        var path = Path.of("not-a-directory");

        try (var filesMock = mockStatic(Files.class)) {
            filesMock.when(() -> Files.exists(path)).thenReturn(true);
            filesMock.when(() -> Files.isDirectory(path)).thenReturn(false);

            var result = fileService.canReadFrom(path);
            assertThat(result).isFalse();
        }
    }

    @Test
    void canReadFrom_should_return_false_if_path_is_not_readable() {
        var fileService = new FileServiceImpl();
        var path = Path.of("not-readable");

        try (var filesMock = mockStatic(Files.class)) {
            filesMock.when(() -> Files.exists(path)).thenReturn(true);
            filesMock.when(() -> Files.isDirectory(path)).thenReturn(true);
            filesMock.when(() -> Files.isReadable(path)).thenReturn(false);

            var result = fileService.canReadFrom(path);
            assertThat(result).isFalse();
        }
    }

    @Test
    void canReadFrom_should_return_true_if_all_conditions_are_met() {
        var fileService = new FileServiceImpl();
        var path = Path.of("readable-dir");

        try (var filesMock = mockStatic(Files.class)) {
            filesMock.when(() -> Files.exists(path)).thenReturn(true);
            filesMock.when(() -> Files.isDirectory(path)).thenReturn(true);
            filesMock.when(() -> Files.isReadable(path)).thenReturn(true);

            var result = fileService.canReadFrom(path);
            assertThat(result).isTrue();
        }
    }

    @Test
    void canWriteTo_should_return_false_if_path_does_not_exist() {
        var fileService = new FileServiceImpl();
        var path = Path.of("does-not-exist");

        try (var filesMock = mockStatic(Files.class)) {
            filesMock.when(() -> Files.exists(path)).thenReturn(false);

            var result = fileService.canWriteTo(path, false);
            assertThat(result).isFalse();
        }
    }

    @Test
    void canWriteTo_should_return_true_if_all_conditions_are_met() {
        var fileService = new FileServiceImpl();
        var path = Path.of("writable-dir");

        try (var filesMock = mockStatic(Files.class)) {
            filesMock.when(() -> Files.exists(path)).thenReturn(true);
            filesMock.when(() -> Files.isDirectory(path)).thenReturn(true);
            filesMock.when(() -> Files.isWritable(path)).thenReturn(true);
            filesMock.when(() -> Files.write(any(Path.class), any(byte[].class))).thenReturn(path);
            filesMock.when(() -> Files.deleteIfExists(any(Path.class))).thenReturn(true);

            var result = fileService.canWriteTo(path, true);
            assertThat(result).isTrue();
            filesMock.verify(() -> Files.write(any(Path.class), any(byte[].class)));
            filesMock.verify(() -> Files.deleteIfExists(any(Path.class)));
        }
    }

    @Test
    void isSameFileSystem_should_return_true_for_single_path() {
        var fileService = new FileServiceImpl();
        var path = Path.of("/a");
        var fileStore = mock(FileStore.class);

        try (var filesMock = mockStatic(Files.class)) {
            filesMock.when(() -> Files.getFileStore(path)).thenReturn(fileStore);

            var result = fileService.isSameFileSystem(List.of(path));
            assertThat(result).isTrue();
        }
    }

    @Test
    void canWriteTo_should_return_false_if_not_directory() {
        var fileService = new FileServiceImpl();
        var path = Path.of("not-a-directory");

        try (var filesMock = mockStatic(Files.class)) {
            filesMock.when(() -> Files.exists(path)).thenReturn(true);
            filesMock.when(() -> Files.isDirectory(path)).thenReturn(false);

            var result = fileService.canWriteTo(path, false);
            assertThat(result).isFalse();
        }
    }

    @Test
    void canWriteTo_should_return_false_if_not_writable() {
        var fileService = new FileServiceImpl();
        var path = Path.of("not-writable");

        try (var filesMock = mockStatic(Files.class)) {
            filesMock.when(() -> Files.exists(path)).thenReturn(true);
            filesMock.when(() -> Files.isDirectory(path)).thenReturn(true);
            filesMock.when(() -> Files.isWritable(path)).thenReturn(false);

            var result = fileService.canWriteTo(path, false);
            assertThat(result).isFalse();
        }
    }

    @Test
    void canWriteTo_should_return_false_if_write_throws_exception() {
        var fileService = new FileServiceImpl();
        var path = Path.of("exception-dir");

        try (var filesMock = mockStatic(Files.class)) {
            filesMock.when(() -> Files.exists(path)).thenReturn(true);
            filesMock.when(() -> Files.isDirectory(path)).thenReturn(true);
            filesMock.when(() -> Files.isWritable(path)).thenReturn(true);
            filesMock.when(() -> Files.write(any(Path.class), any(byte[].class))).thenThrow(new IOException("Disk full"));

            var result = fileService.canWriteTo(path, true);
            assertThat(result).isFalse();
        }
    }
}
