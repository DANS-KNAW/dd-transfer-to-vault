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
package nl.knaw.dans.ttv.core;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.regex.Pattern;

@Slf4j
public class FileNameUtil {
    private static final Pattern DATAVERSE_PATTERN = Pattern.compile(
        "(?<doi>doi-10-[0-9]{4,}-[A-Za-z0-9]{2,}-[A-Za-z0-9]{6})-?" +
            "(?<schema>datacite)?.?" +
            "v(?<major>[0-9]+).(?<minor>[0-9]+)" +
            "(\\.zip)"
    );

    public static String normalizeFilename(Path path) {
        var matcher = DATAVERSE_PATTERN.matcher(path.getFileName().toString());
        if (matcher.matches()) {
            log.debug("Found Dataverse DVE filename: {}", path);
            // Get the creation timestamp
            long creationTime = getCreationTimeUnixTimestamp(path);
            return
                creationTime + "-" +
                    matcher.group("doi") + "-" +
                    String.format("%04d", Integer.parseInt(matcher.group("major"))) + "." +
                    String.format("%04d", Integer.parseInt(matcher.group("minor")))
                    + "-ttv-NULL.zip";
        } else {
            // Try VaaS pattern
            return null;
        }
    }

    public static long getCreationTimeUnixTimestamp(Path path) {
        try {
            BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
            return attrs.creationTime().toInstant().toEpochMilli();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to get creation time of file: " + path, e);
        }
    }

}
