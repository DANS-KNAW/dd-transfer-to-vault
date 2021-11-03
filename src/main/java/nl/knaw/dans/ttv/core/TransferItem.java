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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import java.time.LocalDateTime;

@Entity
@Table(name = "transfer_queue")
@NamedQueries({
        @NamedQuery(name = "TransferItem.findAll", query = "SELECT t FROM TransferItem t"),
})
public class TransferItem {

    public static final String TRANSFER_ITEM_FIND_ALL = "TransferItem.findAll";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @Column(name = "dataset_pid", nullable = false)
    private String datasetPid;

    @Column(name = "dataset_version", nullable = false)
    private String datasetVersion;

    @Column(name = "version_major", nullable = false)
    private int versionMajor;

    @Column(name = "version_minor", nullable = false)
    private int versionMinor;

    @Column(name = "creation_time", nullable = false)
    private LocalDateTime creationTime;

    @Column(name = "metadata_file", nullable = false)
    private String metadataFile;

    public TransferItem(){

    }

    public TransferItem(String datasetPid, String datasetVersion, int versionMajor, int versionMinor, String metadataFile, LocalDateTime creationTime) {
        this.datasetPid = datasetPid;
        this.datasetVersion = datasetVersion;
        this.versionMajor = versionMajor;
        this.versionMinor = versionMinor;
        this.metadataFile = metadataFile;
        this.creationTime = creationTime;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getDatasetPid() {
        return datasetPid;
    }

    public void setDatasetPid(String datasetPid) {
        this.datasetPid = datasetPid;
    }

    public String getDatasetVersion() {
        return datasetVersion;
    }

    public void setDatasetVersion(String datasetVersion) {
        this.datasetVersion = datasetVersion;
    }

    public int getVersionMajor() {
        return versionMajor;
    }

    public void setVersionMajor(int versionMajor) {
        this.versionMajor = versionMajor;
    }

    public int getVersionMinor() {
        return versionMinor;
    }

    public void setVersionMinor(int versionMinor) {
        this.versionMinor = versionMinor;
    }

    public LocalDateTime getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(LocalDateTime creationTime) {
        this.creationTime = creationTime;
    }

    public String getMetadataFile() {
        return metadataFile;
    }

    public void setMetadataFile(String metadataFile) {
        this.metadataFile = metadataFile;
    }
}
