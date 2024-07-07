/*
 * Copyright (C) 2021 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.ttv.core.service;

import nl.knaw.dans.ttv.core.domain.DataFileAttributes;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class DataFileAttributesReaderTest {
    private final FileService fileService = new FileServiceImpl();

    @Test
    public void testReadDataFileAttributes() throws Exception {
        var dataFileAttributesReader = new DataFileAttributesReader(fileService);
        var path = Path.of("src/test/resources/bags/vaas-187618f7-8269-4db6-974d-bf173f0e1c1c-v1.zip");
        var dataFileAttributes = dataFileAttributesReader.readDataFileAttributes(path);

        assertThat(dataFileAttributes).hasSize(4);

        assertThat(dataFileAttributes).containsExactlyInAnyOrder(
            new DataFileAttributes(
                Path.of("data/a/deeper/path/With some file.txt"),
                URI.create("file:///2b5377cf-1194-4043-86ab-720a68383da1"),
                "f750a66151421a62521be6495684fb8384cb4aa0",
                27L),
            new DataFileAttributes(
                Path.of("data/random images/image01.png"),
                URI.create("file:///a1c670f7-1c32-4436-bf55-34b762c7567a"),
                "f100629544e98ad21503b04a276fe6185cb4e9d2",
                422887L),
            new DataFileAttributes(
                Path.of("data/random images/image02.jpeg"),
                URI.create("file:///8db5fbab-8fab-419a-8f31-ffcab1a1e5fc"),
                "4ae4fb20ee161b8026a468160553e623dcea4914",
                13829L),
            new DataFileAttributes(
                Path.of("data/random images/image03.jpeg"),
                URI.create("file:///0401d9ef-bf83-42b2-8ffa-caf09f4d95b1"),
                "0a66ea77834e337e28a043db6d6f3d745c944593",
                2775738L)
        );
    }

}
