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
package nl.knaw.dans.ttv.core.domain;

import java.util.ArrayList;
import java.util.List;

public class ArchiveMetadata {

    private List<ArchiveMetadataPart> parts = new ArrayList<>();

    public List<ArchiveMetadataPart> getParts() {
        return parts;
    }

    public void setParts(List<ArchiveMetadataPart> parts) {
        this.parts = parts;
    }

    @Override
    public String toString() {
        return "ArchiveMetadata{" +
            "parts=" + parts +
            '}';
    }

    public static class ArchiveMetadataPart {
        private String identifier;
        private String checksumAlgorithm;
        private String checksum;

        public ArchiveMetadataPart() {

        }

        public ArchiveMetadataPart(String identifier, String checksumAlgorithm, String checksum) {
            this.identifier = identifier;
            this.checksumAlgorithm = checksumAlgorithm;
            this.checksum = checksum;
        }

        public String getIdentifier() {
            return identifier;
        }

        public void setIdentifier(String identifier) {
            this.identifier = identifier;
        }

        public String getChecksumAlgorithm() {
            return checksumAlgorithm;
        }

        public void setChecksumAlgorithm(String checksumAlgorithm) {
            this.checksumAlgorithm = checksumAlgorithm;
        }

        public String getChecksum() {
            return checksum;
        }

        public void setChecksum(String checksum) {
            this.checksum = checksum;
        }

        @Override
        public String toString() {
            return "ArchiveMetadataPart{" +
                "identifier='" + identifier + '\'' +
                ", checksumAlgorithm='" + checksumAlgorithm + '\'' +
                ", checksum='" + checksum + '\'' +
                '}';
        }
    }
}
