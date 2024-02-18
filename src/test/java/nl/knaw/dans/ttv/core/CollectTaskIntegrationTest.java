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

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import nl.knaw.dans.ttv.core.oaiore.OaiOreMetadataReader;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.FileServiceImpl;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReader;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReaderImpl;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.core.service.TransferItemServiceImpl;
import nl.knaw.dans.ttv.db.TarDao;
import nl.knaw.dans.ttv.db.TransferItemDao;
import org.apache.commons.io.FilenameUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
public class CollectTaskIntegrationTest {
    public static final String TEST_BAG = "doi-10-5072-dar-a7axzpv1.0.zip";
    public static final String TEST_BAG_CHECKSUM = "295ec22a05a3b9ef5917c05ea72495394741d0343337eb763fbca0e87a09aac1";

    private final OaiOreMetadataReader oaiOreMetadataReader = new OaiOreMetadataReader();
    public DAOTestExtension database = DAOTestExtension.newBuilder()
        .addEntityClass(TransferItem.class)
        .addEntityClass(Tar.class)
        .addEntityClass(TarPart.class)
        .build();
    private TransferItemService transferItemService;
    private TransferItemMetadataReader transferItemMetadataReader;
    private FileService fileService;
    private TarDao tarDAO;
    private TransferItemDao transferItemDAO;

    @BeforeEach
    void setUp() {
        tarDAO = new TarDao(database.getSessionFactory());
        transferItemDAO = new TransferItemDao(database.getSessionFactory());
        fileService = new FileServiceImpl();

        transferItemService = new TransferItemServiceImpl(
            transferItemDAO, tarDAO
        );

        transferItemMetadataReader = new TransferItemMetadataReaderImpl(
            fileService,
            oaiOreMetadataReader
        );

    }

    @Test
    void run_should_reject_invalid_filenames() throws Exception {
        try (var fs = MemoryFileSystemBuilder.newLinux().build()) {
            var filePath = fs.getPath("/inbox/deposit1.zip");
            var outboxPath = fs.getPath("/outbox/");

            Files.createDirectories(outboxPath);
            Files.createDirectories(filePath.getParent());
            copyFileToPath(TEST_BAG, filePath);

            var task = new CollectTask(
                filePath,
                outboxPath,
                "datastation1",
                transferItemService,
                transferItemMetadataReader,
                fileService
            );

            task.run();

            assertThat(Files.exists(fs.getPath("/inbox/rejected/deposit1.zip"))).isTrue();

            var error = getErrorMessage(filePath, "rejected");
            assertThat(error).contains("does not match expected pattern");
        }
    }

    @Test
    void run_should_accept_valid_dataverse_file() throws Exception {
        try (var fs = MemoryFileSystemBuilder.newLinux().build()) {
            var filePath = fs.getPath("/inbox/doi-10-5072-dar-vfspuqv1.0.zip");
            var outboxPath = fs.getPath("/outbox/");

            Files.createDirectories(outboxPath);
            Files.createDirectories(filePath.getParent());
            copyFileToPath(TEST_BAG, filePath);

            var task = new CollectTask(
                filePath,
                outboxPath,
                "datastation1",
                transferItemService,
                transferItemMetadataReader,
                fileService
            );

            task.run();

            var records = transferItemDAO.findAll();
            assertThat(records)
                .hasSize(1)
                .extracting(TransferItem::getDveFilename)
                .containsOnly("doi-10-5072-dar-vfspuqv1.0.zip");

            var dbId = records.get(0).getId();
            var allFiles = getAllFiles(filePath.getParent(), outboxPath);

            assertThat(allFiles)
                .hasSize(1)
                .containsOnly(
                    fs.getPath("/outbox/doi-10-5072-dar-vfspuqv1.0-ttv" + dbId + ".zip")
                );
        }
    }

    @Test
    void run_should_accept_valid_vaas_file() throws Exception {
        try (var fs = MemoryFileSystemBuilder.newLinux().build()) {
            var filePath = fs.getPath("/inbox/vaas-ff723a0c-ef1e-45b2-a409-fbeb91e0aa20-v1.zip");
            var outboxPath = fs.getPath("/outbox/");

            Files.createDirectories(outboxPath);
            Files.createDirectories(filePath.getParent());
            copyFileToPath(TEST_BAG, filePath);

            var task = new CollectTask(
                filePath,
                outboxPath,
                "datastation1",
                transferItemService,
                transferItemMetadataReader,
                fileService
            );

            task.run();

            var records = transferItemDAO.findAll();
            assertThat(records)
                .hasSize(1)
                .extracting(TransferItem::getDveFilename)
                .containsOnly("vaas-ff723a0c-ef1e-45b2-a409-fbeb91e0aa20-v1.zip");

            var dbId = records.get(0).getId();
            var allFiles = getAllFiles(filePath.getParent(), outboxPath);

            assertThat(allFiles)
                .hasSize(1)
                .containsOnly(
                    fs.getPath("/outbox/vaas-ff723a0c-ef1e-45b2-a409-fbeb91e0aa20-v1-ttv" + dbId + ".zip")
                );
        }
    }

    @Test
    void run_should_accept_already_existing_file() throws Exception {
        try (var fs = MemoryFileSystemBuilder.newLinux().build()) {
            var filePath = fs.getPath("/inbox/doi-10-5072-dar-vfspuqv1.0.zip");
            var outboxPath = fs.getPath("/outbox/");

            var existing = TransferItem.builder()
                .dveFilename("doi-10-5072-dar-vfspuqv1.0.zip")
                .transferStatus(TransferItem.TransferStatus.TARRING)
                .bagSha256Checksum("different-checksum-than-real-file")
                .creationTime(OffsetDateTime.now())
                .bagId("bagid")
                .ocflObjectVersion(1)
                .dveFilePath("/otherinbox/doi-10-5072-dar-vfspuqv1.0.zip")
                .build();

            transferItemDAO.save(existing);

            Files.createDirectories(outboxPath);
            Files.createDirectories(filePath.getParent());
            copyFileToPath(TEST_BAG, filePath);

            var task = new CollectTask(
                filePath,
                outboxPath,
                "datastation1",
                transferItemService,
                transferItemMetadataReader,
                fileService
            );

            task.run();

            var records = transferItemDAO.findAll();
            assertThat(records)
                .hasSize(2)
                .extracting(TransferItem::getDveFilename)
                .containsOnly("doi-10-5072-dar-vfspuqv1.0.zip");

            var dbId = records.get(1).getId();
            var allFiles = getAllFiles(filePath.getParent(), outboxPath);

            assertThat(allFiles)
                .hasSize(1)
                .containsOnly(
                    fs.getPath("/outbox/doi-10-5072-dar-vfspuqv1.0-ttv" + dbId + ".zip")
                );
        }
    }

    @Test
    void run_should_accept_already_existing_file_with_same_checksum_and_status_collected() throws Exception {
        try (var fs = MemoryFileSystemBuilder.newLinux().build()) {
            var filePath = fs.getPath("/inbox/doi-10-5072-dar-vfspuqv1.0.zip");
            var outboxPath = fs.getPath("/outbox/");

            var existing = TransferItem.builder()
                .dveFilename("doi-10-5072-dar-vfspuqv1.0.zip")
                .transferStatus(TransferItem.TransferStatus.COLLECTED)
                .bagSha256Checksum(TEST_BAG_CHECKSUM)
                .creationTime(OffsetDateTime.now())
                .bagId("bagid")
                .ocflObjectVersion(1)
                .dveFilePath("/otherinbox/doi-10-5072-dar-vfspuqv1.0.zip")
                .build();

            transferItemDAO.save(existing);

            Files.createDirectories(outboxPath);
            Files.createDirectories(filePath.getParent());
            copyFileToPath(TEST_BAG, filePath);

            var task = new CollectTask(
                filePath,
                outboxPath,
                "datastation1",
                transferItemService,
                transferItemMetadataReader,
                fileService
            );

            task.run();

            var records = transferItemDAO.findAll();
            assertThat(records)
                .hasSize(1)
                .extracting(TransferItem::getDveFilename)
                .containsOnly("doi-10-5072-dar-vfspuqv1.0.zip");

            var dbId = records.get(0).getId();
            var allFiles = getAllFiles(filePath.getParent(), outboxPath);

            assertThat(allFiles)
                .hasSize(1)
                .containsOnly(
                    fs.getPath("/outbox/doi-10-5072-dar-vfspuqv1.0-ttv" + dbId + ".zip")
                );
        }
    }

    @Test
    void run_should_reject_file_with_same_checksum_and_other_status() throws Exception {
        try (var fs = MemoryFileSystemBuilder.newLinux().build()) {
            var filePath = fs.getPath("/inbox/doi-10-5072-dar-vfspuqv1.0.zip");
            var outboxPath = fs.getPath("/outbox/");

            var existing = TransferItem.builder()
                .dveFilename("doi-10-5072-dar-vfspuqv1.0.zip")
                .transferStatus(TransferItem.TransferStatus.TARRING)
                .bagSha256Checksum(TEST_BAG_CHECKSUM)
                .creationTime(OffsetDateTime.now())
                .bagId("bagid")
                .ocflObjectVersion(1)
                .dveFilePath("/otherinbox/doi-10-5072-dar-vfspuqv1.0.zip")
                .build();

            transferItemDAO.save(existing);

            Files.createDirectories(outboxPath);
            Files.createDirectories(filePath.getParent());
            copyFileToPath(TEST_BAG, filePath);

            var task = new CollectTask(
                filePath,
                outboxPath,
                "datastation1",
                transferItemService,
                transferItemMetadataReader,
                fileService
            );

            task.run();

            var records = transferItemDAO.findAll();
            assertThat(records)
                .hasSize(1)
                .extracting(TransferItem::getDveFilename)
                .containsOnly("doi-10-5072-dar-vfspuqv1.0.zip");

            var allFiles = getAllFiles(filePath.getParent(), outboxPath);

            assertThat(allFiles)
                .hasSize(2)
                .containsOnly(
                    fs.getPath("/inbox/rejected/doi-10-5072-dar-vfspuqv1.0.zip"),
                    fs.getPath("/inbox/rejected/doi-10-5072-dar-vfspuqv1.0.error.txt")
                );

            var errorMessage = getErrorMessage(filePath, "rejected");
            assertThat(errorMessage)
                .containsIgnoringCase("TransferItem exists already, but does not have expected status of COLLECTED");
        }
    }

    @Test
    void run_should_reject_file_multiple_times() throws Exception {
        try (var fs = MemoryFileSystemBuilder.newLinux().build()) {
            var filePath = fs.getPath("/inbox/doi-10-5072-dar-vfspuqv1.0.zip");
            var outboxPath = fs.getPath("/outbox/");

            var existing = TransferItem.builder()
                .dveFilename("doi-10-5072-dar-vfspuqv1.0.zip")
                .transferStatus(TransferItem.TransferStatus.TARRING)
                .bagSha256Checksum(TEST_BAG_CHECKSUM)
                .creationTime(OffsetDateTime.now())
                .bagId("bagid")
                .ocflObjectVersion(1)
                .dveFilePath("/otherinbox/doi-10-5072-dar-vfspuqv1.0.zip")
                .build();

            transferItemDAO.save(existing);

            Files.createDirectories(outboxPath);
            Files.createDirectories(filePath.getParent());

            var task = new CollectTask(
                filePath,
                outboxPath,
                "datastation1",
                transferItemService,
                transferItemMetadataReader,
                fileService
            );

            copyFileToPath(TEST_BAG, filePath);
            task.run();
            copyFileToPath(TEST_BAG, filePath);
            task.run();

            var allFiles = getAllFiles(filePath.getParent(), outboxPath);

            assertThat(allFiles)
                .hasSize(4)
                .containsOnly(
                    fs.getPath("/inbox/rejected/doi-10-5072-dar-vfspuqv1.0.zip"),
                    fs.getPath("/inbox/rejected/doi-10-5072-dar-vfspuqv1.0.error.txt"),
                    fs.getPath("/inbox/rejected/doi-10-5072-dar-vfspuqv1.0_1.zip"),
                    fs.getPath("/inbox/rejected/doi-10-5072-dar-vfspuqv1.0_1.error.txt")
                );
        }
    }

    @Test
    void run_should_reject_file_that_already_exists_in_outbox() throws Exception {
        try (var fs = MemoryFileSystemBuilder.newLinux().build()) {
            var filePath = fs.getPath("/inbox/doi-10-5072-dar-vfspuqv1.0.zip");
            var outboxPath = fs.getPath("/outbox/");

            Files.createDirectories(outboxPath);
            Files.createDirectories(filePath.getParent());
            copyFileToPath(TEST_BAG, filePath);
            // somehow a file with the exact same name appeared here
            copyFileToPath(TEST_BAG, outboxPath.resolve("doi-10-5072-dar-vfspuqv1.0-ttv1.zip"));

            var task = new CollectTask(
                filePath,
                outboxPath,
                "datastation1",
                transferItemService,
                transferItemMetadataReader,
                fileService
            );

            task.run();

            var records = transferItemDAO.findAll();
            assertThat(records)
                .hasSize(1)
                .extracting(TransferItem::getDveFilename)
                .containsOnly("doi-10-5072-dar-vfspuqv1.0.zip");

            // make sure we don't accidentally overwrite the dve file path
            assertThat(records)
                .extracting(TransferItem::getDveFilePath)
                .containsOnly("/outbox/doi-10-5072-dar-vfspuqv1.0-ttv1.zip");

            var dbId = records.get(0).getId();
            var allFiles = getAllFiles(filePath.getParent(), outboxPath);

            assertThat(allFiles)
                .hasSize(3)
                .containsOnly(
                    fs.getPath("/outbox/doi-10-5072-dar-vfspuqv1.0-ttv" + dbId + ".zip"),
                    fs.getPath("/inbox/rejected/doi-10-5072-dar-vfspuqv1.0.zip"),
                    fs.getPath("/inbox/rejected/doi-10-5072-dar-vfspuqv1.0.error.txt")
                );

            var error = getErrorMessage(filePath, "rejected");
            assertThat(error)
                .containsIgnoringCase("file already exists");
        }
    }

    private void copyFileToPath(String filename, Path filePath) throws Exception {
        var p = Objects.requireNonNull(getClass().getResource("/bags/" + filename)).getPath();
        var input = Files.newInputStream(Path.of(p));

        Files.copy(input, filePath);
    }

    private String getErrorMessage(Path filePath, String errorType) throws Exception {
        var name = FilenameUtils.removeExtension(filePath.getFileName().toString());
        var errorPath = filePath.getParent().resolve(errorType).resolve(name + ".error.txt");

        return Files.readString(errorPath, StandardCharsets.UTF_8);
    }

    private List<Path> getAllFiles(Path inbox, Path outbox) throws IOException {
        var stream1 = Files.walk(inbox)
            .filter(Files::isRegularFile);

        var stream2 = Files.walk(outbox)
            .filter(Files::isRegularFile);

        return Stream.concat(stream1, stream2)
            .collect(Collectors.toList());
    }

}
