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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.zip.ZipFile;

public interface FileService {

    Path moveFile(Path current, Path newPath) throws IOException;

    Object getFilesystemAttribute(Path path, String property) throws IOException;

    String calculateChecksum(Path path) throws IOException;

    long getFileSize(Path path) throws IOException;

    long getPathSize(Path path) throws IOException;

    ZipFile openZipFile(Path path) throws IOException;

    /**
     * Returns an input stream for the entry under the base folder of the given zip file. The ZIP file is assumed to contain
     * a single folder at the root level, and the entry is assumed to be under that folder.
     *
     * @param datasetVersionExport the zip file
     * @param subpath              the path of the entry under the base folder
     * @return an input stream for the entry
     * @throws IOException              if the entry cannot be read
     * @throws IllegalArgumentException if the entry is not found or if more than one base folder is found
     */
    InputStream getEntryUnderBaseFolder(ZipFile datasetVersionExport, Path subpath) throws IOException;

    Path moveFileAtomically(Path filePath, Path newPath) throws IOException;

    void ensureDirectoryExists(Path path) throws IOException;

    void rejectFile(Path path, Throwable exception) throws IOException;

    boolean exists(Path path);

    boolean canRead(Path path, int timeout) throws TimeoutException;

    boolean canRead(Path path);

    boolean canWrite(Path path);

    FileStore getFileStore(Path path) throws IOException;

    Path addCreationTimeToFileName(Path path) throws IOException;

    void cleanup(Path dir, Pattern pattern) throws IOException;
}
