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

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileServiceTest extends TestDirFixture {

    @Test
    void isDifferentFileSystem() throws IOException {
        var fileService = new FileServiceImpl();
        var result = fileService.isSameFileSystem(Path.of("/tmp"), Path.of("/dev/null"));
        assertFalse(result);
    }

    @Test
    void isSameFileSystem() throws IOException {
        var fileService = new FileServiceImpl();
        var result = fileService.isSameFileSystem(Path.of("."), Path.of(".."));
        assertTrue(result);
    }

    @Test
    void canNotWriteTo() {
        var fileService = new FileServiceImpl();
        var result = fileService.canWriteTo(testDir.resolve("does-not-exist"));
        assertFalse(result);
    }

    @Test
    void canWriteTo() {
        var fileService = new FileServiceImpl();
        var result = fileService.canWriteTo(testDir);
        assertTrue(result);
    }
}
