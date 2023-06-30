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

import nl.knaw.dans.ttv.core.InvalidTransferItemException;
import nl.knaw.dans.ttv.db.TransferItem;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.*;

class TransferItemValidatorImplTest {

    @Test
    void validateTransferItem() {
        var transferItem = TransferItem.builder()
            .datasetPid("pid1")
            .dveFilePath("path/to1.zip")
            .creationTime(OffsetDateTime.now())
            .transferStatus(TransferItem.TransferStatus.COLLECTED)
            .build();
        transferItem.setDatasetVersion("2.1");
        transferItem.setBagId("urn:uuid:1eb8d2fe-b8fa-4a15-9770-731cae6af9ac");
        transferItem.setNbn("urn:nbn:suffix");
        assertDoesNotThrow(() -> new TransferItemValidatorImpl().validateTransferItem(transferItem));
    }

    @Test
    void validateInvalidTransferItemBecauseDatasetVersionIsNull() {
        var transferItem = TransferItem.builder()
            .datasetPid("pid1")
            .dveFilePath("path/to1.zip")
            .creationTime(OffsetDateTime.now())
            .transferStatus(TransferItem.TransferStatus.COLLECTED)
            .build();
        transferItem.setBagId("urn:uuid:1eb8d2fe-b8fa-4a15-9770-731cae6af9ac");
        transferItem.setNbn("urn:nbn:suffix");
        assertThrows(InvalidTransferItemException.class, () -> new TransferItemValidatorImpl().validateTransferItem(transferItem));
    }

    @Test
    void validateInvalidTransferItemBecauseBagIdIsInvalid() {
        var transferItem = TransferItem.builder()
            .datasetPid("pid1")
            .dveFilePath("path/to1.zip")
            .creationTime(OffsetDateTime.now())
            .transferStatus(TransferItem.TransferStatus.COLLECTED)
            .build();
        transferItem.setDatasetVersion("2.1");
        transferItem.setNbn("urn:nbn:suffix");
        assertThrows(InvalidTransferItemException.class, () -> new TransferItemValidatorImpl().validateTransferItem(transferItem));
    }

    @Test
    void validateInvalidTransferItemBecauseNbnIsInvalid() {
        var transferItem = TransferItem.builder()
            .datasetPid("pid1")
            .dveFilePath("path/to1.zip")
            .creationTime(OffsetDateTime.now())
            .transferStatus(TransferItem.TransferStatus.COLLECTED)
            .build();
        transferItem.setDatasetVersion("2.1");
        transferItem.setBagId("urn:uuid:1eb8d2fe-b8fa-4a15-9770-731cae6af9ac");
        assertThrows(InvalidTransferItemException.class, () -> new TransferItemValidatorImpl().validateTransferItem(transferItem));
    }

    @Test
    void validateInvalidTransferItemBecauseVersionIsIncorrect() {
        var transferItem = TransferItem.builder()
            .datasetPid("pid1")
            .dveFilePath("path/to1.zip")
            .creationTime(OffsetDateTime.now())
            .transferStatus(TransferItem.TransferStatus.COLLECTED)
            .datasetVersion("0.0")
            .nbn("urn:nbn:suffix")
            .bagId("urn:uuid:1eb8d2fe-b8fa-4a15-9770-731cae6af9ac")
            .build();
        assertThrows(InvalidTransferItemException.class, () -> new TransferItemValidatorImpl().validateTransferItem(transferItem));
    }


    @Test
    void isValidVersion() {
        var validator = new TransferItemValidatorImpl();

        assertTrue(validator.isValidVersion("1.0"));
        assertTrue(validator.isValidVersion("3.2"));
        assertTrue(validator.isValidVersion("16.235"));
    }

    @Test
    void isInValidVersion() {
        var validator = new TransferItemValidatorImpl();

        assertFalse(validator.isValidVersion(".0"));
        assertFalse(validator.isValidVersion("0."));
        assertFalse(validator.isValidVersion("-3.2"));
        assertFalse(validator.isValidVersion(" 18.123"));
        assertFalse(validator.isValidVersion(" completely different"));
    }

    @Test
    void isValidVersionMajorAndMinor() {
        var validator = new TransferItemValidatorImpl();
        assertTrue(validator.isValidVersionMajorAndMinor(1, 0));
        assertTrue(validator.isValidVersionMajorAndMinor(100, 3));
        assertTrue(validator.isValidVersionMajorAndMinor(123, 2));
        assertTrue(validator.isValidVersionMajorAndMinor(1, 16));
    }

    @Test
    void isInValidVersionMajorAndMinor() {
        var validator = new TransferItemValidatorImpl();
        assertFalse(validator.isValidVersionMajorAndMinor(1, -1));
        assertFalse(validator.isValidVersionMajorAndMinor(0, 3));
        assertFalse(validator.isValidVersionMajorAndMinor(-1, 1));
        assertFalse(validator.isValidVersionMajorAndMinor(0, 0));
    }

    @Test
    void isValidBagId() {
        var validator = new TransferItemValidatorImpl();
        assertTrue(validator.isValidBagId("urn:uuid:1eb8d2fe-b8fa-4a15-9770-731cae6af9ac"));
    }

    @Test
    void isInValidBagId() {
        var validator = new TransferItemValidatorImpl();
        assertFalse(validator.isValidBagId("urn:uuid:1eb8d2fe-b8fa-4a15-9770-731cae6af9ac "));
        // note that this one has an invalid character (X)
        assertFalse(validator.isValidBagId("urn:uuid:1xb8d2fe-b8fa-4a15-9770-731cae6af9ac"));
        assertFalse(validator.isValidBagId("urn:uuid:test"));
        assertFalse(validator.isValidBagId("urn:uuid:"));
        assertFalse(validator.isValidBagId("hello"));
    }

    @Test
    void isValidNbn() {
        var validator = new TransferItemValidatorImpl();
        assertTrue(validator.isValidNbn("urn:nbn:something"));
        assertTrue(validator.isValidNbn("urn:nbn:a"));
    }
}
