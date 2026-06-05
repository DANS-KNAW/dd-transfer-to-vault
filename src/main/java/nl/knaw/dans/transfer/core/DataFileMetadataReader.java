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
import nl.knaw.dans.bagit.domain.Bag;
import nl.knaw.dans.bagit.domain.FetchItem;
import nl.knaw.dans.bagit.hash.StandardSupportedAlgorithms;
import nl.knaw.dans.bagit.reader.BagReader;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.ProviderNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
public class DataFileMetadataReader {
    private final FileService fileService;

    public List<DataFileMetadata> readDataFileAttributes(Path dveZip) throws IOException {
        try (var zipFs = fileService.newFileSystem(dveZip)) {
            if (zipFs == null) {
                throw new IOException("Failed to open file system for " + dveZip);
            }
            var rootDir = zipFs.getRootDirectories().iterator().next();
            try (var topLevelDirStream = fileService.list(rootDir)) {
                var topLevelDir = topLevelDirStream
                    .filter(fileService::isDirectory)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No top-level directory found in DVE"));

                var pidMappingContent = fileService.newInputStream(topLevelDir.resolve("metadata/pid-mapping.txt"));
                var pidMapping = IOUtils.toString(pidMappingContent, StandardCharsets.UTF_8);
                var pathToPidMap = readPathToPidMapping(pidMapping);

                BagReader reader = new BagReader();
                Bag bag = reader.read(topLevelDir);

                var pathToSha1Map = bag.getPayLoadManifests().stream()
                    .filter(m -> m.getAlgorithm() == StandardSupportedAlgorithms.SHA1)
                    .findFirst()
                    .map(m -> m.getFileToChecksumMap().entrySet().stream()
                        .collect(Collectors.toMap(entry -> topLevelDir.relativize(entry.getKey()).toString(), Map.Entry::getValue)))
                    .orElse(Map.of());

                var pathToFetchItemMap = bag.getItemsToFetch().stream()
                    .collect(Collectors.toMap(item -> topLevelDir.relativize(item.getPath()).toString(), item -> item));

                return pathToPidMap.entrySet().stream()
                    .filter(e -> pathToSha1Map.containsKey(e.getKey()))
                    .map(entry -> {
                        var path = entry.getKey();
                        var pid = entry.getValue();
                        var sha1 = pathToSha1Map.get(path);
                        var fetchItem = pathToFetchItemMap.get(path);
                        long size;
                        if (fetchItem != null) {
                            size = fetchItem.length == null ? -1L : fetchItem.length;
                        }
                        else {
                            try {
                                size = fileService.readAttributes(topLevelDir.resolve(path), java.nio.file.attribute.BasicFileAttributes.class).size();
                            }
                            catch (IOException e) {
                                size = -1L;
                            }
                        }
                        return new DataFileMetadata(path, pid, sha1, size);
                    })
                    .toList();
            }
        }
        catch (ProviderNotFoundException e) {
            throw new RuntimeException("The file system provider is not found. Probably not a ZIP file: " + dveZip, e);
        }
        catch (Exception e) {
            throw new IOException("Error reading data file attributes from " + dveZip, e);
        }
    }

    Map<String, URI> readPathToPidMapping(String pidToPathMapping) throws IOException {
        var pathToPidMap = new HashMap<String, URI>();
        try (var lines = pidToPathMapping.lines()) {
            lines.map(line -> line.split("\\s+", 2))
                .forEach(parts -> {
                    if (parts[1].equals("data/")) {
                        return; // Entry for the complete dataset can be ignored
                    }
                    var pathStr = parts[1];
                    pathToPidMap.put(pathStr, URI.create(parts[0]));
                });

        }
        return pathToPidMap;
    }
}