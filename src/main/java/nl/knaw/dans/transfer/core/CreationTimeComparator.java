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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;

public class CreationTimeComparator implements Comparator<Path> {
    private static CreationTimeComparator instance;

    private CreationTimeComparator() {
        // Private constructor to prevent instantiation
    }

    public static CreationTimeComparator getInstance() {
        if (instance == null) {
            instance = new CreationTimeComparator();
        }
        return instance;
    }

    @Override
    public int compare(Path o1, Path o2) {
        try {
            return Long.compare(Files.readAttributes(o1, BasicFileAttributes.class).creationTime().toMillis(),
                    Files.readAttributes(o2, BasicFileAttributes.class).creationTime().toMillis());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
