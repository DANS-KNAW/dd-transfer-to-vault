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

import lombok.Getter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a DVE file name. This is one of:
 * <ul>
 *     <li>A ZIP file exported by Dataverse</li>
 *     <li>A ZIP file exported by dd-vault-ingest-flow</li>
 * </ul>
 */
@Getter
public class DveFileName {
    private static final String ORDER_NUMBER = "((?<ordernumber>\\d+)-)";

    private static final String DOI = "(?<doi>doi-10-\\d{4,}-\\w{2,}-\\w{2,})";

    private static final String VERSION = "v(?<major>\\d+).(?<minor>\\d+)";

    private static final String EXT = "(\\.zip)";

    private static final String UUID = "(?<uuid>[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{12})";

    private static final String OCFL_OBJECT_VERSION_NR = "(-v(?<ocflobjectversionnr>\\d+))?";

    private static final Pattern DATAVERSE_DVE_NAME = Pattern.compile(
        "^" + ORDER_NUMBER + "?" + DOI + VERSION + EXT + "$"
    );

    private static final Pattern VAAS_DVE_NAME = Pattern.compile(
        "^" + ORDER_NUMBER + "?vaas-" + UUID + OCFL_OBJECT_VERSION_NR + EXT + "$"
    );

    private final String filename;

    private final Long orderNumber;

    private final String doi;

    private final Integer major;

    private final Integer minor;

    private final String uuid;

    private final Integer ocflObjectVersionNr;

    private final boolean dve;

    public DveFileName(String filename) {
        this.filename = filename;
        Matcher matcher = DATAVERSE_DVE_NAME.matcher(filename);
        if (matcher.matches()) {
            orderNumber = matcher.group("ordernumber") != null ? Long.parseLong(matcher.group("ordernumber")) : null;
            doi = matcher.group("doi");
            major = Integer.parseInt(matcher.group("major"));
            minor = Integer.parseInt(matcher.group("minor"));
            uuid = null;
            ocflObjectVersionNr = null;
            dve = true;
        }
        else {
            matcher = VAAS_DVE_NAME.matcher(filename);
            if (matcher.matches()) {
                orderNumber = matcher.group("ordernumber") != null ? Long.parseLong(matcher.group("ordernumber")) : null;
                doi = null;
                major = null;
                minor = null;
                uuid = matcher.group("uuid");
                ocflObjectVersionNr = matcher.group("ocflobjectversionnr") != null ? Integer.parseInt(matcher.group("ocflobjectversionnr")) : null;
                dve = true;
            }
            else {
                orderNumber = null;
                doi = null;
                major = null;
                minor = null;
                uuid = null;
                ocflObjectVersionNr = null;
                dve = false;
            }
        }
    }
}
