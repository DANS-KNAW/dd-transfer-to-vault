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

import lombok.AllArgsConstructor;
import nl.knaw.dans.ttv.core.domain.DataFileAttributes;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@AllArgsConstructor
public class DataFileAttributesReader {
    private final FileService fileService;

    public List<DataFileAttributes> readDataFileAttributes(Path dveZip) throws IOException {
        try (var datasetVersionExport = fileService.openZipFile(dveZip)) {
            var pidMappingContent = fileService.openFileFromZip(datasetVersionExport, Path.of("metadata/pid-mapping.txt"));
            var sha1ManifestContent = fileService.openFileFromZip(datasetVersionExport, Path.of("manifest-sha1.txt"));
            var pidMapping = IOUtils.toString(pidMappingContent, StandardCharsets.UTF_8);
            var sha1Manifest = IOUtils.toString(sha1ManifestContent, StandardCharsets.UTF_8);
            var pathToPidMap = readPathToPidMapping(pidMapping);
            var pathToSha1Map = readPathToSha1Mapping(sha1Manifest);
            var pathToSizeMap = readPathToSizeMapping(datasetVersionExport, Path.of("data/"));

            return pathToPidMap.entrySet().stream()
                .map(entry -> {
                    var path = entry.getKey();
                    var pid = entry.getValue();
                    var sha1 = pathToSha1Map.get(path);
                    var size = pathToSizeMap.get(path);
                    return new DataFileAttributes(path, pid, sha1, size);
                })
                .toList();
        }
    }

    Map<Path, Long> readPathToSizeMapping(ZipFile dve, Path relativeToPath) throws IOException {
        Map<Path, Long> pathToSizeMap = new HashMap<>();
        var entries = dve.entries();
        while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            if (!entry.isDirectory()) {
                Path entryPath = Path.of(entry.getName());
                // Paths outside the data directory are not considered, but the full entry path is still used as key
                if (entryPath.startsWith(relativeToPath)) {
                    long size = entry.getSize();
                    pathToSizeMap.put(entryPath, size);
                }
            }
        }
        return pathToSizeMap;
    }

    Map<Path, URI> readPathToPidMapping(String pidToPathMapping) throws IOException {
        var pathToPidMap = new HashMap<Path, URI>();
        try (var lines = pidToPathMapping.lines()) {
            lines.map(line -> line.split("\\s+", 2))
                .forEach(parts -> {
                    if (parts[1].equals("data/")) {
                        return;
                    }
                    pathToPidMap.put(Path.of(parts[1]), URI.create(parts[0]));
                });

        }
        return pathToPidMap;
    }

    Map<Path, String> readPathToSha1Mapping(String sha1ToPathMapping) throws IOException {
        HashMap<Path, String> pathToSha1Map;
        try (var lines = sha1ToPathMapping.lines()) {
            pathToSha1Map = lines
                .map(line -> line.split("\\s+", 2))
                .collect(Collectors.toMap(parts -> Path.of(parts[1]), parts -> parts[0], (a, b) -> b, HashMap::new));
        }
        return pathToSha1Map;
    }
}