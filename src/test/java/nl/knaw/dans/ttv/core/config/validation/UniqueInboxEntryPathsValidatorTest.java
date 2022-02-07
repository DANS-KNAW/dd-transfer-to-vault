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
package nl.knaw.dans.ttv.core.config.validation;

import nl.knaw.dans.ttv.core.config.CollectConfiguration;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UniqueInboxEntryPathsValidatorTest {

    @Test
    void testIsValid() {
        var validator = new UniqueInboxEntryPathsValidator();
        var entries = List.of(
            new CollectConfiguration.InboxEntry("name1", Path.of("tmp/folder1")),
            new CollectConfiguration.InboxEntry("name2", Path.of("tmp/folder2")),
            new CollectConfiguration.InboxEntry("name3", Path.of("tmp/folder3"))
        );

        var result = validator.isValid(entries, null);

        assertTrue(result);
    }

    @Test
    void testIsInValid() {
        var validator = new UniqueInboxEntryPathsValidator();
        var entries = List.of(
            new CollectConfiguration.InboxEntry("name1", Path.of("tmp/folder1")),
            new CollectConfiguration.InboxEntry("name2", Path.of("tmp/folder2")),
            new CollectConfiguration.InboxEntry("name3", Path.of("tmp/folder1"))
        );

        var result = validator.isValid(entries, null);

        assertFalse(result);
    }

    /**
     * this tests if it only checks the paths, not the names
     */
    @Test
    void testIsValidDespiteDuplicateNames() {
        var validator = new UniqueInboxEntryPathsValidator();
        var entries = List.of(
            new CollectConfiguration.InboxEntry("name", Path.of("tmp/folder1")),
            new CollectConfiguration.InboxEntry("name", Path.of("tmp/folder2")),
            new CollectConfiguration.InboxEntry("name", Path.of("tmp/folder3"))
        );

        var result = validator.isValid(entries, null);

        assertTrue(result);
    }
}
