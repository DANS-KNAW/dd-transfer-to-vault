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

import nl.knaw.dans.lib.util.ZipUtil;
import nl.knaw.dans.ttv.core.domain.DataFileAttributes;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class DataFileAttributesReaderTest {
    private final FileService fileService = new FileServiceImpl();

    @Test
    public void testReadDataFileAttributes() throws Exception {
        Files.createDirectories(Path.of("target/test"));
        ZipUtil.zipDirectory(Path.of("src/test/resources/bags/audiences"), Path.of("target/test/vaas-03ca6301-1b10-4a40-a33c-d5696a7ee3f0-v1.zip"), true);
        
        var dataFileAttributesReader = new DataFileAttributesReader(fileService);
        var path = Path.of("target/test/vaas-03ca6301-1b10-4a40-a33c-d5696a7ee3f0-v1.zip");
        var dataFileAttributes = dataFileAttributesReader.readDataFileAttributes(path);

        assertThat(dataFileAttributes).hasSize(4);

        assertThat(dataFileAttributes).containsExactlyInAnyOrder(
            new DataFileAttributes(
                Path.of("data/a/deeper/path/With some file.txt"),
                URI.create("file:///984bf2c6-fd72-423f-8a91-a871a9273888"),
                "f750a66151421a62521be6495684fb8384cb4aa0",
                27L),
            new DataFileAttributes(
                Path.of("data/random images/image01.png"),
                URI.create("file:///92a7ebce-739d-4d06-929c-e52021a60b04"),
                "f100629544e98ad21503b04a276fe6185cb4e9d2",
                422887L),
            new DataFileAttributes(
                Path.of("data/random images/image02.jpeg"),
                URI.create("file:///d6e9a1bc-c74b-4852-b80b-ed4b16c358e0"),
                "4ae4fb20ee161b8026a468160553e623dcea4914",
                13829L),
            new DataFileAttributes(
                Path.of("data/random images/image03.jpeg"),
                URI.create("file:///df4ac453-437f-4cc7-a016-29e780012c37"),
                "0a66ea77834e337e28a043db6d6f3d745c944593",
                2775738L)
        );
    }

}
