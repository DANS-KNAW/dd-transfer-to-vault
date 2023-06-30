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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.annotations.ColumnDefault;
import org.hibernate.proxy.HibernateProxy;

import javax.persistence.*;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Entity
@Table(name = "tar")
@Getter
@Setter
@ToString
@RequiredArgsConstructor
public class Tar {
    @Id
    @Column(name = "tar_uuid", nullable = false)
    private String tarUuid;
    @Column(name = "vault_path")
    private String vaultPath;
    @Column(name = "datetime_created")
    private OffsetDateTime created;
    @Column(name = "datetime_confirmed_archived")
    private OffsetDateTime datetimeConfirmedArchived;
    @OneToMany(mappedBy = "tar", cascade = CascadeType.ALL)
    @ToString.Exclude
    private List<TransferItem> transferItems = new ArrayList<>();
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
    @ToString.Exclude
    private List<TarPart> tarParts = new ArrayList<>();
    @Column(name = "transfer_attempt")
    @ColumnDefault("0")
    private int transferAttempt;

    public Tar(String tarUuid, TarStatus status, boolean confirmCheckInProgress) {
        this.tarUuid = tarUuid;
        this.tarStatus = status;
        this.confirmCheckInProgress = confirmCheckInProgress;
    }

    public Tar(String uuid) {
        this.setTarUuid(uuid);
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        Class<?> oEffectiveClass = o instanceof HibernateProxy ? ((HibernateProxy) o).getHibernateLazyInitializer().getPersistentClass() : o.getClass();
        Class<?> thisEffectiveClass = this instanceof HibernateProxy ? ((HibernateProxy) this).getHibernateLazyInitializer().getPersistentClass() : this.getClass();
        if (thisEffectiveClass != oEffectiveClass) return false;
        Tar tar = (Tar) o;
        return getTarUuid() != null && Objects.equals(getTarUuid(), tar.getTarUuid());
    }

    @Override
    public final int hashCode() {
        return getClass().hashCode();
    }

    public enum TarStatus {
        TARRING,
        OCFLTARCREATED,
        OCFLTARFAILED,
        CONFIRMEDARCHIVED,
    }
}
