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

import lombok.AllArgsConstructor;
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
public class DataFileMetadataReader {
    private final FileService fileService;

    public List<DataFileMetadata> readDataFileAttributes(Path dveZip) throws IOException {
        try (var datasetVersionExport = fileService.openZipFile(dveZip)) {
            var pidMappingContent = fileService.getEntryUnderBaseFolder(datasetVersionExport, Path.of("metadata/pid-mapping.txt"));
            var sha1ManifestContent = fileService.getEntryUnderBaseFolder(datasetVersionExport, Path.of("manifest-sha1.txt"));
            var pidMapping = IOUtils.toString(pidMappingContent, StandardCharsets.UTF_8);
            var sha1Manifest = IOUtils.toString(sha1ManifestContent, StandardCharsets.UTF_8);
            var pathToPidMap = readPathToPidMapping(pidMapping);
            var pathToSha1Map = readPathToSha1Mapping(sha1Manifest);
            var pathToSizeMap = readPathToSizeMapping(datasetVersionExport);

            return pathToPidMap.entrySet().stream()
                .filter(e -> pathToSha1Map.containsKey(e.getKey()))
                .map(entry -> {
                    var path = entry.getKey();
                    var pid = entry.getValue();
                    var sha1 = pathToSha1Map.get(path);
                    var size = pathToSizeMap.get(path);
                    return new DataFileMetadata(path, pid, sha1, size);
                })
                .toList();
        }
    }

    Map<Path, Long> readPathToSizeMapping(ZipFile dve) throws IOException {
        var pathToSizeMap = new HashMap<Path, Long>();
        var dveEntries = dve.stream().toList();
        var baseFolder = findBaseFolder(dveEntries);
        dveEntries.stream()
            .filter(entry -> !entry.isDirectory())
            .forEach(entry -> {
                var entryPath = Path.of(entry.getName());
                var size = entry.getSize();
                pathToSizeMap.put(baseFolder.relativize(entryPath), size);
            });
        return pathToSizeMap;
    }

    /*
     * There should be a common base folder for the whole ZIP file. If there are multiple base folders, an exception should be thrown.
     * Inside the common base folder there should be a "data" folder. Return the path to the "data" folder.
     */
    Path findBaseFolder(List<? extends ZipEntry> entries) {
        var baseFolders = entries.stream()
            .map(ZipEntry::getName)
            .map(name -> name.split("/")[0])
            .distinct()
            .toList();
        if (baseFolders.size() != 1) {
            throw new IllegalArgumentException("There should be a common base folder for the whole ZIP file.");
        }
        return Path.of(baseFolders.get(0));
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