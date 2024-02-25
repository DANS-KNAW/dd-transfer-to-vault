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

package nl.knaw.dans.ttv.config;

import io.dropwizard.core.Configuration;
import io.dropwizard.db.DataSourceFactory;
import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Data
@EqualsAndHashCode(callSuper = true)
public class DdTransferToVaultConfig extends Configuration {

    @Valid
    @NotNull
    private DataSourceFactory database = new DataSourceFactory();

    @Valid
    @NotNull
    private CollectConfig collect;

    @Valid
    @NotNull
    private ExtractMetadataConfig extractMetadata;

    @Valid
    @NotNull
    private SendToVaultConfig sendToVault;

    @Valid
    @NotNull
    private ConfirmArchivedConfig confirmArchived;

    @Valid
    @NotNull
    private VaultCatalogConfig vaultCatalog;

    @Valid
    @NotNull
    private DataVaultConfig dataVault;
}
