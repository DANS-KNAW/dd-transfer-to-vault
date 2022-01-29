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
package nl.knaw.dans.ttv.core.dto;

import java.time.LocalDateTime;

public class FilesystemAttributes {
    private LocalDateTime creationTime;
    private String bagChecksum;
    private long bagSize;

    public LocalDateTime getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(LocalDateTime creationTime) {
        this.creationTime = creationTime;
    }

    public String getBagChecksum() {
        return bagChecksum;
    }

    public void setBagChecksum(String bagChecksum) {
        this.bagChecksum = bagChecksum;
    }

    public long getBagSize() {
        return bagSize;
    }

    public void setBagSize(long bagSize) {
        this.bagSize = bagSize;
    }

    @Override
    public String toString() {
        return "FilesystemAttributes{" +
            "creationTime=" + creationTime +
            ", bagChecksum='" + bagChecksum + '\'' +
            ", bagSize=" + bagSize +
            '}';
    }
}
