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

package nl.knaw.dans.ttv;

import io.dropwizard.core.Configuration;
import io.dropwizard.db.DataSourceFactory;
import lombok.Data;
import nl.knaw.dans.ttv.core.config.CollectConfiguration;
import nl.knaw.dans.ttv.core.config.ConfirmArchivedConfiguration;
import nl.knaw.dans.ttv.core.config.CreateOcflTarConfiguration;
import nl.knaw.dans.ttv.core.config.DataArchiveConfiguration;
import nl.knaw.dans.ttv.core.config.ExtractMetadataConfiguration;
import nl.knaw.dans.ttv.core.config.VaultCatalogConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Data
public class DdTransferToVaultConfiguration extends Configuration {

    @Valid
    @NotNull
    private DataSourceFactory database = new DataSourceFactory();
    @Valid
    @NotNull
    private CollectConfiguration collect;
    @Valid
    @NotNull
    private ExtractMetadataConfiguration extractMetadata;
    @Valid
    @NotNull
    private CreateOcflTarConfiguration createOcflTar;
    @Valid
    @NotNull
    private ConfirmArchivedConfiguration confirmArchived;
    @Valid
    @NotNull
    private DataArchiveConfiguration dataArchive;
    @Valid
    @NotNull
    private VaultCatalogConfiguration vaultCatalog;
}
