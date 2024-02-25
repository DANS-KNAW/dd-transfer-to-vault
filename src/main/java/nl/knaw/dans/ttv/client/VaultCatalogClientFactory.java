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
package nl.knaw.dans.ttv.client;

import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.core.setup.Environment;
import nl.knaw.dans.ttv.config.DdTransferToVaultConfig;
import nl.knaw.dans.vaultcatalog.client.invoker.ApiClient;
import nl.knaw.dans.vaultcatalog.client.resources.OcflObjectVersionApi;
import nl.knaw.dans.vaultcatalog.client.resources.TarApi;

public class VaultCatalogClientFactory {

    public VaultCatalogClient createVaultCatalogClient(DdTransferToVaultConfig configuration, Environment environment) {
        var client = new JerseyClientBuilder(environment)
            .using(configuration.getVaultCatalog().getHttpClient())
            .build("vault-catalog");

        var apiClient = new ApiClient();
        apiClient.setHttpClient(client);
        apiClient.setBasePath(configuration.getVaultCatalog().getUrl().toString());

        var tarApi = new TarApi(apiClient);
        var ocflObjectVersionApi = new OcflObjectVersionApi(apiClient);

        return new VaultCatalogClientImpl(tarApi, ocflObjectVersionApi);
    }
}
