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

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * A Dataset Version Export (DVE) and auxiliary files. The DVE is the only mandatory file. The other files are searched next to the DVE or constructed from the DVE. This class is intended to provide
 * lightweight access to the DVE and its properties. It is not intended to be a full-fledged DVE reader or writer.
 */
@Slf4j
public class TransferItem {
    private static final String PROPERTIES_SUFFIX = ".properties";
    private static final String ERROR_LOG_SUFFIX = "-error.log";

    private static final String KEY_CREATION_TIME = "creationTime";
    private static final String KEY_MD5 = "md5";
    private static final String KEY_OCFL_OBJECT_VERSION = "ocflObjectVersion";
    private static final String KEY_NBN = "nbn";

    private Path dve;
    private Path properties;

    public TransferItem(Path dve) {
        this.dve = dve;
        this.properties = initProperties(dve);
    }

    private static Path initProperties(Path dve) {
        try {
            var properties = dve.resolveSibling(dve.getFileName() + PROPERTIES_SUFFIX);
            if (Files.notExists(properties)) {
                var props = new Properties();
                props.setProperty(KEY_CREATION_TIME, getCreationTime(dve).toString());
                props.setProperty(KEY_MD5, calculateMd5(dve));
                // Save
                try (var out = Files.newOutputStream(properties)) {
                    props.store(out, null);
                }
            }
            return properties;
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to initialize properties file", e);
        }
    }

    /**
     * Moves the DVE to a new directory. If the file already exists with the same MD5 hash, it will be deleted.
     *
     * @param dir the directory to move the DVE to
     * @param e   the exception to log in the error log file
     * @throws IOException if an I/O error occurs
     */
    public void moveToDir(Path dir, Exception e) throws IOException {
        var newLocation = findFreeName(dir, dve);
        var newPropertiesFile = newLocation.resolveSibling(newLocation.getFileName() + PROPERTIES_SUFFIX);
        if (Files.exists(newLocation)) {
            log.error("File already exists: {}", newLocation);
        }
        else {
            Files.move(properties, newPropertiesFile);
            Files.move(dve, newLocation);
            dve = newLocation;
            properties = newPropertiesFile;
        }
        if (e != null) {
            var errorLogFile = newLocation.resolveSibling(newLocation.getFileName() + ERROR_LOG_SUFFIX);
            writeStackTrace(errorLogFile, e);
        }
    }

    private static void writeStackTrace(Path errorLog, Exception e) throws IOException {
        try (var writer = Files.newBufferedWriter(errorLog)) {
            e.printStackTrace(new java.io.PrintWriter(writer));
        }
    }

    private Path findFreeName(Path targetDir, Path dve) throws IOException {
        var fileName = dve.getFileName().toString();
        var dotIndex = fileName.lastIndexOf('.');
        var baseName = (dotIndex == -1) ? fileName : fileName.substring(0, dotIndex);
        var extension = (dotIndex == -1) ? "" : fileName.substring(dotIndex);
        var sequenceNumber = 0;
        Path newPath;
        do {
            if (sequenceNumber == 0) {
                newPath = targetDir.resolve(baseName + extension);
            }
            else {
                newPath = targetDir.resolve(baseName + "-" + sequenceNumber + extension);
            }
            sequenceNumber++;
            if (Files.exists(newPath) && new TransferItem(newPath).getMd5().equals(this.getMd5())) {
                break;
            }
        }
        while (Files.exists(newPath));
        return newPath;
    }

    public void moveToDir(Path dir) throws IOException {
        moveToDir(dir, null);
    }

    private void setProperty(String key, String value) {
        try {
            var props = new Properties();
            if (Files.exists(properties)) {
                try (var in = Files.newInputStream(properties)) {
                    props.load(in);
                }
            }
            props.setProperty(key, value);
            try (var out = Files.newOutputStream(properties)) {
                props.store(out, null);
            }
        }
        catch (Exception ex) {
            throw new RuntimeException("Failed to set property", ex);
        }
    }

    public String getProperty(String key) {
        try {
            var props = new Properties();
            if (properties != null && Files.exists(properties)) {
                try (var in = Files.newInputStream(properties)) {
                    props.load(in);
                }
            }
            return props.getProperty(key);
        }
        catch (Exception ex) {
            throw new RuntimeException("Failed to get property", ex);
        }
    }

    public int getOcflObjectVersion() {
        Object ocflObjectVersion = getProperty(KEY_OCFL_OBJECT_VERSION);
        if (ocflObjectVersion == null) {
            return -1;
        }
        else {
            try {
                return Integer.parseInt(ocflObjectVersion.toString());
            }
            catch (NumberFormatException e) {
                throw new IllegalStateException("Invalid OCFL object version: " + ocflObjectVersion, e);
            }
        }
    }

    public void setOcflObjectVersion(int i) {
        if (i < 1) {
            throw new IllegalArgumentException("OCFL object version must be greater than 0");
        }
        setProperty(KEY_OCFL_OBJECT_VERSION, String.valueOf(i));
    }

    public String getNbn() {
        return getProperty(KEY_NBN);
    }

    public void setNbn(String nbn) {
        setProperty(KEY_NBN, nbn);
    }

    public String getMd5() {
        return getProperty(KEY_MD5);
    }

    private static Object getCreationTime(Path path) throws IOException {
        return Files.getAttribute(path, "creationTime", java.nio.file.LinkOption.NOFOLLOW_LINKS);
    }

    private static String calculateMd5(Path path) throws IOException {
        try {
            var md5 = java.security.MessageDigest.getInstance("MD5");
            try (var is = Files.newInputStream(path)) {
                var digest = md5.digest(is.readAllBytes());
                var hexString = new StringBuilder();
                for (byte b : digest) {
                    hexString.append(String.format("%02x", b));
                }
                return hexString.toString();
            }
        }
        catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm not found", e);
        }
    }
}
