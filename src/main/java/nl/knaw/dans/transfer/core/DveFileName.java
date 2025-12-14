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

import lombok.Getter;

import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a DVE filename to extract the creation date-time and OCFL object version. Expected filename pattern: *[_<creationDateTime>][_v<ocflObjectVersion>][-<index>].zip where <creationDateTime> is
 * an ISO-8601 formatted date-time string and <ocflObjectVersion> and <index> are integers.
 */
public class DveFileName {
    private static final String BASE_NAME_SECTION = "^(?<baseName>.*?)";
    private static final String CREATION_TIME_SECTION = "(_(?<creationTime>[0-9]+))?";
    private static final String OCFL_OBJECT_VERSION_SECTION = "(_v(?<ocflObjectVersion>[0-9]+))?";
    private static final String INDEX_SECTION = "(-(?<index>[0-9]+))?";
    private static final String EXTENSION_SECTION = "(?<extension>\\.[^.]+$)";

    private static final Pattern fileNamePattern = Pattern.compile(
        BASE_NAME_SECTION + CREATION_TIME_SECTION + OCFL_OBJECT_VERSION_SECTION + INDEX_SECTION + EXTENSION_SECTION);

    @Getter
    private final Path path;

    @Getter
    private final String baseName;
    @Getter
    private final OffsetDateTime creationTime;
    @Getter
    private final Integer ocflObjectVersion;
    @Getter
    private final Integer index;

    private DveFileName(Path path, String baseName, OffsetDateTime creationTime, Integer ocflObjectVersion, Integer index) {
        this.baseName = baseName;
        this.creationTime = creationTime;
        this.ocflObjectVersion = ocflObjectVersion;
        this.index = index;
        this.path = path.resolveSibling(getFileName());
    }

    public DveFileName(Path dve) {
        this.path = dve;
        var matcher = fileNamePattern.matcher(dve.getFileName().toString());
        if (matcher.find()) {
            this.baseName = parseBaseName(matcher);
            this.creationTime = parseCreationTime(matcher);
            this.ocflObjectVersion = parseOcflObjectVersion(matcher);
            this.index = parseIndex(matcher);
        }
        else {
            throw new IllegalArgumentException("DVE filename does not match expected pattern: " + dve.getFileName().toString());
        }
    }

    public String getFileName() {
        return String.format("%s%s%s%s.zip",
            baseName,
            creationTime != null ? "_" + creationTime.toInstant().toEpochMilli() : "",
            ocflObjectVersion != null ? "_v" + ocflObjectVersion : "",
            index != null ? "-" + index : ""
        );
    }

    private static String parseBaseName(Matcher matcher) {
        return matcher.group("baseName");
    }

    private static OffsetDateTime parseCreationTime(Matcher matcher) {
        var creationTimeGroup = matcher.group("creationTime");
        return creationTimeGroup == null ? null : OffsetDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(creationTimeGroup)), ZoneOffset.UTC);
    }

    private static Integer parseOcflObjectVersion(Matcher matcher) {
        var ocflObjectVersionGroup = matcher.group("ocflObjectVersion");
        return ocflObjectVersionGroup == null ? null : Integer.valueOf(ocflObjectVersionGroup);
    }

    private static Integer parseIndex(Matcher matcher) {
        var indexGroup = matcher.group("index");
        return indexGroup == null ? null : Integer.valueOf(indexGroup);
    }

    public DveFileName withCreationTime(OffsetDateTime creationDateTime) {
        if (this.creationTime != null && !this.creationTime.equals(creationDateTime)) {
            throw new IllegalStateException("Creation time is already set for DVE: " + path.getFileName().toString());
        }
        return new DveFileName(
            this.path,
            this.baseName,
            creationDateTime,
            this.ocflObjectVersion,
            this.index
        );
    }

    public DveFileName withOcflObjectVersion(int ocflObjectVersion) {
        if (this.ocflObjectVersion != null && this.ocflObjectVersion != ocflObjectVersion) {
            throw new IllegalStateException("OCFL object version is already set for DVE: " + path.getFileName().toString());
        }
        return new DveFileName(
            this.path,
            this.baseName,
            this.creationTime,
            ocflObjectVersion,
            this.index
        );
    }

    public DveFileName withIndex(int index) {
        return new DveFileName(
            this.path,
            this.baseName,
            this.creationTime,
            this.ocflObjectVersion,
            index
        );
    }
}
