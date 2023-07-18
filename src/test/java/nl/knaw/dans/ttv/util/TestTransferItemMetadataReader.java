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
package nl.knaw.dans.ttv.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.knaw.dans.ttv.core.InvalidTransferItemException;
import nl.knaw.dans.ttv.core.domain.FileContentAttributes;
import nl.knaw.dans.ttv.core.domain.FilenameAttributes;
import nl.knaw.dans.ttv.core.domain.FilesystemAttributes;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReaderImpl;
import org.mockito.Mockito;

import java.nio.file.Path;

public class TestTransferItemMetadataReader extends TransferItemMetadataReaderImpl {

    private final static FileService fileService = Mockito.mock(FileService.class);

    private final FilenameAttributes filenameAttributes;
    private final FilesystemAttributes filesystemAttributes;
    private final FileContentAttributes fileContentAttributes;

    public TestTransferItemMetadataReader(
        FilenameAttributes filenameAttributes,
        FilesystemAttributes filesystemAttributes,
        FileContentAttributes fileContentAttributes
    ) {
        super(new ObjectMapper(), fileService);

        this.fileContentAttributes = fileContentAttributes;
        this.filesystemAttributes = filesystemAttributes;
        this.filenameAttributes = filenameAttributes;
    }

    @Override
    public FilenameAttributes getFilenameAttributes(Path path) throws InvalidTransferItemException {
        if (this.filenameAttributes != null) {
            return this.filenameAttributes;
        }

        return super.getFilenameAttributes(path);
    }

    @Override
    public FilesystemAttributes getFilesystemAttributes(Path path) throws InvalidTransferItemException {
        if (this.filesystemAttributes != null) {
            return this.filesystemAttributes;
        }

        return super.getFilesystemAttributes(path);
    }

    @Override
    public FileContentAttributes getFileContentAttributes(Path path) throws InvalidTransferItemException {
        if (this.fileContentAttributes != null) {
            return this.fileContentAttributes;
        }

        return super.getFileContentAttributes(path);
    }
}
