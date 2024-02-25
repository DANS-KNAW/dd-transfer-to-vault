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
import nl.knaw.dans.datavault.client.invoker.ApiClient;
import nl.knaw.dans.datavault.client.resources.DefaultApi;
import nl.knaw.dans.ttv.config.DataVaultConfig;

public class DataVaultClientFactory {

    public DefaultApi createDataVaultClient(DataVaultConfig configuration, Environment environment) {
        var client = new JerseyClientBuilder(environment)
            .using(configuration.getHttpClient())
            .build("data-vault");

        var apiClient = new ApiClient();
        apiClient.setHttpClient(client);
        apiClient.setBasePath(configuration.getUrl().toString());

        return new DefaultApi(apiClient);
    }
}
