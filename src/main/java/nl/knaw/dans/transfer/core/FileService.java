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
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.stream.Stream;
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
     * Opens a file, returning an input stream to read from the file.
     *
     * @param path the path to the file
     * @return a new input stream
     * @throws IOException if an I/O error occurs
     */
    InputStream newInputStream(Path path) throws IOException;

    /**
     * Returns a lazily populated Stream, the elements of which are the entries in the directory.
     *
     * @param dir the path to the directory
     * @return the Stream of elements in the directory
     * @throws IOException if an I/O error occurs
     */
    Stream<Path> list(Path dir) throws IOException;

    /**
     * Write a String to a file.
     *
     * @param path    the path to the file
     * @param content the content to write
     * @throws IOException if an I/O error occurs
     */
    void writeString(Path path, String content) throws IOException;

    /**
     * Reads a file's attributes as a bulk operation.
     *
     * @param path the path to the file
     * @param type the Class of the file attributes required
     * @param <A>  the BasicFileAttributes type
     * @return the file attributes
     * @throws IOException if an I/O error occurs
     */
    <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type) throws IOException;

    /**
     * Moves a file from oldLocation to newLocation.
     *
     * @param oldLocation the current location of the file
     * @param newLocation the new location of the file
     * @throws IOException if the file cannot be moved
     */
    void move(Path oldLocation, Path newLocation) throws IOException;

    /**
     * Deletes a file.
     *
     * @param path the path to the file to delete
     * @throws IOException if the file cannot be deleted
     */
    void delete(Path path) throws IOException;

    /**
     * Moves a file from oldLocation to newLocation. The move is atomic if the underlying file system supports it.
     *
     *
     * @param oldLocation the current location of the file
     * @param newLocation the new location of the file
     * @throws IOException if the file cannot be moved
     */
    void moveAtomically(Path oldLocation, Path newLocation) throws IOException;

    /**
     * Flushes the file system buffers for the given file, ensuring that all changes are written to the storage device.
     *
     * @param file the file to fsync
     * @throws IOException if the fsync operation fails
     */
    void fsyncFile(Path file) throws IOException;

    /**
     * Flushes the file system buffers for the given directory, ensuring that all changes are written to the storage device.
     *
     * @param dir the directory to fsync
     * @throws IOException if the fsync operation fails
     */
    void fsyncDirectory(Path dir) throws IOException;


    /**
     * Creates a file system for the given path.
     *
     * @param path the path to the file
     * @return a new file system
     * @throws IOException if an I/O error occurs
     */
    java.nio.file.FileSystem newFileSystem(Path path) throws IOException;

    /**
     * Opens or creates a file, returning an output stream that may be used to write bytes to the file.
     *
     * @param path the path to the file
     * @return a new output stream
     * @throws IOException if an I/O error occurs
     */
    java.io.OutputStream newOutputStream(Path path) throws IOException;

    /**
     * Creates a new directory.
     *
     * @param dir the directory to create
     * @throws IOException if an I/O error occurs
     */
    void createDirectories(Path dir) throws IOException;

    /**
     * Checks if the given path is a regular file.
     *
     * @param path the path to check
     * @return true if the path is a regular file, false otherwise
     */
    boolean isRegularFile(Path path);

    /**
     * Checks if the given path is a directory.
     *
     * @param path the path to check
     * @return true if the path is a directory, false otherwise
     */
    boolean isDirectory(Path path);

    /**
     * Checks if the given path exists.
     *
     * @param path the path to check
     * @return true if the path exists, false otherwise
     */
    boolean exists(Path path);


    /**
     * Checks if the given paths are on the same file system.
     *
     * @param paths the paths to check
     * @return true if all paths are on the same file system, false otherwise
     */
    boolean isSameFileSystem(java.util.Collection<Path> paths);

    /**
     * Checks if the given path is readable by the current user.
     *
     * @param path the path to check
     * @return true if the path is readable, false otherwise
     */
    boolean canReadFrom(Path path);

    /**
     * Checks if the given path is writable by the current user.
     *
     * @param path the path to check
     * @return true if the path is writable, false otherwise
     */
    boolean canWriteTo(Path path);

    /**
     * Ensures that the given directory exists, creating it if necessary.
     *
     * @param dir the directory to check
     * @throws IOException if the directory cannot be created
     */
    void ensureDirectoryExists(Path dir) throws IOException;

    /**
     * Finds an existing target directory for the given NBN in the destination root, or creates a new one if none exists.
     * The target directory name consists of the NBN followed by a random string of 6 uppercase letters.
     *
     * @param targetNbn the NBN to search for
     * @param destinationRoot the root directory to search in
     * @return the existing or newly created target directory
     */
    Path findOrCreateTargetDir(String targetNbn, Path destinationRoot);
}
