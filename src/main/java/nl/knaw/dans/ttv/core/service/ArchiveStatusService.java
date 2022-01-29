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
import java.util.Map;

public interface ArchiveStatusService {

    /**
     * File status based on the surfsara docs
     * A file is stored on tape if the state is either DUAL or OFFLINE
     *
     *     REG: Regular files are user files residing only on disk
     *     MIG: Migrating files are files which are being copied from disk to tape
     *     UNM: Unmigrating files are files which are being copied from tape to disk
     *     Migrated files can be either of the following:
     *         DUL: Dual-state files whose data resides both online and offline
     *         OFL: Offline files whose data is no longer on disk
     */
    enum FileStatus {
        REGULAR("REG"),
        MIGRATING("MIG"),
        UNMIGRATING("UNM"),
        DUAL("DUL"),
        OFFLINE("OFL"),
        ARCHIVING("ARC"),
        NONMIGRATABLE("NMG"),
        PARTIAL("PAR"),
        INVALID("INV"),
        UNKNOWN("N/A"),
        ;

        private String value;

        FileStatus(String value) {
            this.value = value;
        }

        public static FileStatus fromString(String code) {
            for (var value: FileStatus.values()) {
                if (value.value.equals(code)) {
                    return value;
                }
            }

            return UNKNOWN;
        }
    }

    /**
     * Returns a map of paths with their corresponding file status
     * @param id
     * @return
     */
    Map<String, FileStatus> getFileStatus(String id) throws IOException, InterruptedException;
}
