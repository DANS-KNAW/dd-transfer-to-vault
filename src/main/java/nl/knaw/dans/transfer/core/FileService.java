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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.zip.ZipFile;

public interface FileService {

    ZipFile openZipFile(Path path) throws IOException;

    /**
     * Returns an input stream for the entry under the base folder of the given zip file. The ZIP file is assumed to contain a single folder at the root level, and the entry is assumed to be under
     * that folder.
     *
     * @param datasetVersionExport the zip file
     * @param subpath              the path of the entry under the base folder
     * @return an input stream for the entry
     * @throws IOException              if the entry cannot be read
     * @throws IllegalArgumentException if the entry is not found, or if more than one base folder is found
     */
    InputStream getEntryUnderBaseFolder(ZipFile datasetVersionExport, Path subpath) throws IOException;

    /**
     * Moves a file from oldLocation to newLocation. The move is atomic if the underlying file system supports it.
     *
     *
     * @param oldLocation the current location of the file
     * @param newLocation the new location of the file
     * @throws IOException if the file cannot be moved
     */
    void moveAtomically(Path oldLocation, Path newLocation) throws IOException;

    void fsyncFile(Path file) throws IOException;

    void fsyncDirectory(Path dir) throws IOException;
}
