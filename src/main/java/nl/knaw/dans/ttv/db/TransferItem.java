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
package nl.knaw.dans.ttv.db;

import org.hibernate.annotations.ColumnDefault;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Objects;

@Entity
@Table(name = "transfer_queue", uniqueConstraints={@UniqueConstraint(columnNames = {"dataset_pid" , "version_major", "version_minor"})})
public class TransferItem {

    private static final Logger log = LoggerFactory.getLogger(TransferItem.class);

    public enum TransferStatus {
        EXTRACT,
        COLLECTED,
        TARRING,
        OCFLTARCREATED,
        CONFIRMEDARCHIVED
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @Column(name = "dataset_pid", nullable = false)
    private String datasetPid;

    @Column(name = "dataset_version")
    private String datasetVersion;

    @Column(name = "version_major", nullable = false)
    private int versionMajor;

    @Column(name = "version_minor", nullable = false)
    private int versionMinor;

    @Column(name = "creation_time", nullable = false)
    private LocalDateTime creationTime;

    @Column(name = "dve_file_path", nullable = false)
    private String dveFilePath;

    @Column(name = "bag_id")
    private String bagId;

    @Column(name = "nbn")
    private String nbn;

    @Column(name = "other_id")
    private String otherId;

    @Column(name = "other_id_version")
    private String otherIdVersion;

    @Column(name = "sword_token")
    private String swordToken;

    @Column(name = "dataset_dv_instance")
    private String datasetDvInstance;

    @Column(name = "bag_checksum")
    private String bagChecksum;

    @Column(name = "queue_date")
    private LocalDateTime queueDate;

    @Column(name = "bag_size")
    private long bagSize;

    @Enumerated(EnumType.STRING)
    @Column(name = "transfer_status", nullable = false)
    private TransferStatus transferStatus;

    @Column(name = "oai_ore", length = 10000)
    @org.hibernate.annotations.Type( type="materialized_blob" )
    private byte[] oaiOre;

    @Column(name = "pid_mapping", length = 10000)
    @org.hibernate.annotations.Type( type="materialized_blob" )
    private byte[] pidMapping;

    @Column(name = "aip_tar_entry_name")
    private String aipTarEntryName;

    @Column(name = "aips_tar")
    private String aipsTar;

    @Column(name = "bag_deposit_date")
    private LocalDateTime bagDepositDate;

    @Column(name = "confirm_check_in_progress")
    @ColumnDefault("false")
    private boolean confirmCheckInProgress;

    public TransferItem(){

    }

    public TransferItem(String datasetPid, int versionMajor, int versionMinor, String dveFilePath, LocalDateTime creationTime, TransferStatus transferStatus) {
        this.datasetPid = datasetPid;
        this.versionMajor = versionMajor;
        this.versionMinor = versionMinor;
        this.dveFilePath = dveFilePath;
        this.creationTime = creationTime;
        this.transferStatus = transferStatus;
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

    public String getDveFilePath() {
        return dveFilePath;
    }

    public void setDveFilePath(String dveFilePath) {
        this.dveFilePath = dveFilePath;
    }

    public String getBagId() {
        return bagId;
    }

    public void setBagId(String bagId) {
        this.bagId = bagId;
    }

    public String getNbn() {
        return nbn;
    }

    public void setNbn(String nbn) {
        this.nbn = nbn;
    }

    public String getOtherId() {
        return otherId;
    }

    public void setOtherId(String otherId) {
        this.otherId = otherId;
    }

    public String getOtherIdVersion() {
        return otherIdVersion;
    }

    public void setOtherIdVersion(String otherIdVersion) {
        this.otherIdVersion = otherIdVersion;
    }

    public String getSwordToken() {
        return swordToken;
    }

    public void setSwordToken(String swordToken) {
        this.swordToken = swordToken;
    }

    public String getDatasetDvInstance() {
        return datasetDvInstance;
    }

    public void setDatasetDvInstance(String datasetDvInstance) {
        this.datasetDvInstance = datasetDvInstance;
    }

    public String getBagChecksum() {
        return bagChecksum;
    }

    public void setBagChecksum(String bagChecksum) {
        this.bagChecksum = bagChecksum;
    }

    public LocalDateTime getQueueDate() {
        return queueDate;
    }

    public void setQueueDate(LocalDateTime queueDate) {
        this.queueDate = queueDate;
    }

    public long getBagSize() {
        return bagSize;
    }

    public void setBagSize(long bagSize) {
        this.bagSize = bagSize;
    }

    public TransferStatus getTransferStatus() {
        return transferStatus;
    }

    public void setTransferStatus(TransferStatus transferStatus) {
        this.transferStatus = transferStatus;
    }

    public byte[] getOaiOre() {
        return oaiOre;
    }

    public void setOaiOre(byte[] oaiOre) {
        this.oaiOre = oaiOre;
    }

    public byte[] getPidMapping() {
        return pidMapping;
    }

    public void setPidMapping(byte[] pidMapping) {
        this.pidMapping = pidMapping;
    }

    public String getAipTarEntryName() {
        return aipTarEntryName;
    }

    public void setAipTarEntryName(String aipTarEntryName) {
        this.aipTarEntryName = aipTarEntryName;
    }

    public String getAipsTar() {
        return aipsTar;
    }

    public void setAipsTar(String aipsTar) {
        this.aipsTar = aipsTar;
    }

    public LocalDateTime getBagDepositDate() {
        return bagDepositDate;
    }

    public void setBagDepositDate(LocalDateTime bagDepositDate) {
        this.bagDepositDate = bagDepositDate;
    }

    public boolean isConfirmCheckInProgress() {
        return confirmCheckInProgress;
    }

    public void setConfirmCheckInProgress(boolean confirmCheckInProgress) {
        this.confirmCheckInProgress = confirmCheckInProgress;
    }

    @Override
    public String toString() {
        return "TransferItem{" +
                "id=" + id +
                ", datasetPid='" + datasetPid + '\'' +
                ", datasetVersion='" + datasetVersion + '\'' +
                ", versionMajor=" + versionMajor +
                ", versionMinor=" + versionMinor +
                ", creationTime=" + creationTime +
                ", dveFilePath='" + dveFilePath + '\'' +
                ", bagId='" + bagId + '\'' +
                ", nbn='" + nbn + '\'' +
                ", otherId='" + otherId + '\'' +
                ", otherIdVersion='" + otherIdVersion + '\'' +
                ", swordToken='" + swordToken + '\'' +
                ", datasetDvInstance='" + datasetDvInstance + '\'' +
                ", bagChecksum='" + bagChecksum + '\'' +
                ", queueDate=" + queueDate +
                ", bagSize=" + bagSize +
                ", transferStatus=" + transferStatus +
                ", aipTarEntryName='" + aipTarEntryName + '\'' +
                ", aipsTar='" + aipsTar + '\'' +
                ", bagDepositDate=" + bagDepositDate +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransferItem that = (TransferItem) o;
        return versionMajor == that.versionMajor && versionMinor == that.versionMinor && bagSize == that.bagSize && datasetPid.equals(that.datasetPid) && Objects.equals(datasetVersion, that.datasetVersion) && creationTime.equals(that.creationTime) && dveFilePath.equals(that.dveFilePath) && Objects.equals(bagId, that.bagId) && Objects.equals(nbn, that.nbn) && Objects.equals(otherId, that.otherId) && Objects.equals(otherIdVersion, that.otherIdVersion) && Objects.equals(swordToken, that.swordToken) && Objects.equals(datasetDvInstance, that.datasetDvInstance) && Objects.equals(bagChecksum, that.bagChecksum) && Objects.equals(queueDate, that.queueDate) && transferStatus == that.transferStatus && Arrays.equals(oaiOre, that.oaiOre) && Arrays.equals(pidMapping, that.pidMapping) && Objects.equals(aipTarEntryName, that.aipTarEntryName) && Objects.equals(aipsTar, that.aipsTar) && Objects.equals(bagDepositDate, that.bagDepositDate) && confirmCheckInProgress == that.confirmCheckInProgress;
    }
}
