package nl.knaw.dans.ttv.db;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "tar")
public class TarItem {

    @Id
    @Column(name = "tar_uuid", nullable = false)
    private UUID tarUuid;

    @Column(name = "vault_path", nullable = false)
    private String vaultPath;

    @Column(name = "checksum_algorithm")
    private String checksumAlgorithm;

    @Column(name = "checksum_value")
    private String checksumValue;

    @Column(name = "archival_date", nullable = false)
    private LocalDateTime archivalDate;

    public UUID getTarUuid() {
        return tarUuid;
    }

    public void setTarUuid(UUID tarUuid) {
        this.tarUuid = tarUuid;
    }

    public String getVaultPath() {
        return vaultPath;
    }

    public void setVaultPath(String vaultPath) {
        this.vaultPath = vaultPath;
    }

    public String getChecksumAlgorithm() {
        return checksumAlgorithm;
    }

    public void setChecksumAlgorithm(String checksumAlgorithm) {
        this.checksumAlgorithm = checksumAlgorithm;
    }

    public String getChecksumValue() {
        return checksumValue;
    }

    public void setChecksumValue(String checksumValue) {
        this.checksumValue = checksumValue;
    }

    public LocalDateTime getArchivalDate() {
        return archivalDate;
    }

    public void setArchivalDate(LocalDateTime archivalDate) {
        this.archivalDate = archivalDate;
    }
}
