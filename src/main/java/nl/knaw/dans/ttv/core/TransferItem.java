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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.io.FilenameUtils;
import org.hibernate.Hibernate;
import org.hibernate.annotations.TypeDef;
import org.hibernate.annotations.TypeDefs;
import org.hibernate.type.TextType;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import java.time.OffsetDateTime;
import java.util.Objects;

@Entity
@Table(name = "transfer_item",
       uniqueConstraints = { @UniqueConstraint(columnNames = { "bag_id", "ocfl_object_version" }) }
)
@TypeDefs({
    @TypeDef(name = "string", defaultForType = String.class, typeClass = TextType.class)
})
@Getter
@Setter
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransferItem {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "bag_id")
    private String bagId;

    @Column(name = "ocfl_object_version")
    private Integer ocflObjectVersionNumber;

    @Column(name = "dve_filename", nullable = false)
    private String dveFilename;

    @Column(name = "dve_file_path", nullable = false)
    private String dveFilePath;

    @Column(name = "nbn")
    private String nbn;

    @Column(name = "sword_token")
    private String swordToken;

    @Column(name = "data_supplier")
    private String dataSupplier;

    @Column(name = "ocfl_object_path")
    private String ocflObjectPath;

    @Column(name = "datastation")
    private String datastation;

    @Column(name = "dataverse_pid")
    private String dataversePid;

    @Column(name = "dataverse_pid_version")
    private String dataversePidVersion;

    @Column(name = "other_id")
    private String otherId;

    @Column(name = "other_id_version")
    private String otherIdVersion;

    @Column(name = "metadata")
    @ToString.Exclude
    private String metadata;

    @Column(name = "filepid_to_local_path")
    @ToString.Exclude
    private String filepidToLocalPath;

    @Column
    private String deaccessioned;

    @Column
    private String exporter;

    @Column(name = "exporter_version")
    private String exporterVersion;

    @Column(name = "creation_time", nullable = false)
    private OffsetDateTime creationTime;

    @Column(name = "bag_sha256_checksum")
    private String bagSha256Checksum;

    @Column(name = "queue_timestamp")
    private OffsetDateTime queueTimestamp;

    @Column(name = "bag_size")
    private long bagSize;

    @Enumerated(EnumType.STRING)
    @Column(name = "transfer_status", nullable = false)
    private TransferStatus transferStatus;

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || Hibernate.getClass(this) != Hibernate.getClass(o))
            return false;
        TransferItem that = (TransferItem) o;
        return getId() != null && Objects.equals(getId(), that.getId());
    }

    @Override
    public final int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    public String getCanonicalFilename() {
        if (this.getId() == null || this.getDveFilename() == null) {
            throw new IllegalStateException("Cannot create canonical filename for transfer item without id and dataset identifier");
        }

        var name = FilenameUtils.removeExtension(this.getDveFilename());

        return String.format("%s-ttv%s.zip", name, getId());
    }

    public enum TransferStatus {
        COLLECTED,
        METADATA_EXTRACTED,
        SENT_TO_VAULT
    }
}
