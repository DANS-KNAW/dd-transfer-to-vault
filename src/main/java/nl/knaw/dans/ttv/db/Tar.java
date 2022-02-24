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

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "tar")
public class Tar {
    @Id
    @Column(name = "tar_uuid", nullable = false)
    private String tarUuid;
    @Column(name = "vault_path")
    private String vaultPath;
    @Column(name = "datetime_created")
    private LocalDateTime created;
    @Column(name = "datetime_confirmed_archived")
    private LocalDateTime datetimeConfirmedArchived;
    @OneToMany(mappedBy = "aipsTar", cascade = CascadeType.ALL)
    private List<TransferItem> transferItems;
    @Column(name = "archive_in_progress")
    @ColumnDefault("false")
    private boolean archiveInProgress;
    @Column(name = "confirm_check_in_progress")
    @ColumnDefault("false")
    private boolean confirmCheckInProgress;
    @Enumerated(EnumType.STRING)
    @Column(name = "tar_status")
    private TarStatus tarStatus;
    @OneToMany(mappedBy = "tar", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<TarPart> tarParts = new ArrayList<>();
    @Column(name = "transfer_attempt")
    @ColumnDefault("0")
    private int transferAttempt;

    public Tar(String tarUuid, TarStatus status, boolean confirmCheckInProgress) {
        this.tarUuid = tarUuid;
        this.tarStatus = status;
        this.confirmCheckInProgress = confirmCheckInProgress;
        this.setTransferItems(new ArrayList<>());
        this.setTarParts(new ArrayList<>());
    }

    public Tar() {
        this.setTransferItems(new ArrayList<>());
        this.setTarParts(new ArrayList<>());
    }

    public Tar(String uuid) {
        this.setTarUuid(uuid);
    }

    public boolean isArchiveInProgress() {
        return archiveInProgress;
    }

    public void setArchiveInProgress(boolean archiveInProgress) {
        this.archiveInProgress = archiveInProgress;
    }

    public int getTransferAttempt() {
        return transferAttempt;
    }

    public void setTransferAttempt(int transferAttempt) {
        this.transferAttempt = transferAttempt;
    }

    public String getTarUuid() {
        return tarUuid;
    }

    public void setTarUuid(String tarUuid) {
        this.tarUuid = tarUuid;
    }

    public String getVaultPath() {
        return vaultPath;
    }

    public void setVaultPath(String vaultPath) {
        this.vaultPath = vaultPath;
    }

    public LocalDateTime getCreated() {
        return created;
    }

    public void setCreated(LocalDateTime created) {
        this.created = created;
    }

    public List<TransferItem> getTransferItems() {
        return transferItems;
    }

    public void setTransferItems(List<TransferItem> transferItems) {
        this.transferItems = transferItems;
    }

    public boolean isConfirmCheckInProgress() {
        return confirmCheckInProgress;
    }

    public void setConfirmCheckInProgress(boolean confirmCheckInProgress) {
        this.confirmCheckInProgress = confirmCheckInProgress;
    }

    public TarStatus getTarStatus() {
        return tarStatus;
    }

    public void setTarStatus(TarStatus tarStatus) {
        this.tarStatus = tarStatus;
    }

    public List<TarPart> getTarParts() {
        return tarParts;
    }

    public void setTarParts(List<TarPart> tarParts) {
        this.tarParts = tarParts;
    }

    public LocalDateTime getDatetimeConfirmedArchived() {
        return datetimeConfirmedArchived;
    }

    public void setDatetimeConfirmedArchived(LocalDateTime datetimeConfirmedArchived) {
        this.datetimeConfirmedArchived = datetimeConfirmedArchived;
    }

    @Override
    public String toString() {
        return "Tar{" +
            "tarUuid='" + tarUuid + '\'' +
            ", vaultPath='" + vaultPath + '\'' +
            ", created=" + created +
            ", datetimeConfirmedArchived=" + datetimeConfirmedArchived +
            ", archiveInProgress=" + archiveInProgress +
            ", confirmCheckInProgress=" + confirmCheckInProgress +
            ", tarStatus=" + tarStatus +
            ", transferAttempt=" + transferAttempt +
            '}';
    }

    public enum TarStatus {
        TARRING,
        OCFLTARCREATED,
        OCFLTARFAILED,
        CONFIRMEDARCHIVED,
    }
}
