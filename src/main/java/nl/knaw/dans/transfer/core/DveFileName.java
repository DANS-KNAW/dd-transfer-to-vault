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
import java.util.regex.Pattern;

/**
 * Parses a DVE filename to extract the creation date-time and OCFL object version. Expected filename pattern: *_<creationDateTime>_v<ocflObjectVersion>.zip where <creationDateTime> is an ISO-8601
 * formatted date-time string and <ocflObjectVersion> is an integer.
 */
public class DveFileName {
    private static final Pattern fileNamePatter = Pattern.compile("^.*?(_(?<creationTime>[0-9]+))?(_v(?<ocflObjectVersion>[0-9]+))?.zip$");

    @Getter
    private final OffsetDateTime creationDateTime;
    @Getter
    private final Integer ocflObjectVersion;

    public DveFileName(Path dve) {
        var matcher = fileNamePatter.matcher(dve.getFileName().toString());
        if (matcher.find()) {
            var creationTimeGroup = matcher.group("creationTime");
            this.creationDateTime = creationTimeGroup == null ? null : OffsetDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(creationTimeGroup)), ZoneOffset.UTC);
            var ocflObjectVersionGroup = matcher.group("ocflObjectVersion");
            this.ocflObjectVersion = ocflObjectVersionGroup == null ? null : Integer.valueOf(ocflObjectVersionGroup);
        }
        else {
            throw new IllegalArgumentException("DVE filename does not match expected pattern: " + dve.getFileName().toString());
        }
    }
}
