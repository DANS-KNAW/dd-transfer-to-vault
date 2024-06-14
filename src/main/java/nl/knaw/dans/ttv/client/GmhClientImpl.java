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

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.gmh.client.api.NbnLocationsObjectDto;
import nl.knaw.dans.gmh.client.invoker.ApiException;
import nl.knaw.dans.gmh.client.resources.UrnnbnIdentifierApi;
import nl.knaw.dans.ttv.core.TransferItem;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

@AllArgsConstructor
@Slf4j
public class GmhClientImpl implements GmhClient {
    private final UrnnbnIdentifierApi api;

    private final URI catalogBaseUrl;

    @Override
    public void registerNbn(TransferItem transferItem) throws IOException {
        var landingPageUrl = catalogBaseUrl + transferItem.getNbn();
        log.debug("Registering NBN {} with landing page URL {}", transferItem.getNbn(), landingPageUrl);
        var nbnLocationDto = new NbnLocationsObjectDto()
            .identifier(transferItem.getNbn())
            .addLocationsItem(landingPageUrl);
        try {
            api.createNbnLocations(nbnLocationDto);
        }
        catch (ApiException e) {
            // TODO: implement automatic retry?
            log.warn("Failed to register NBN {} with landing page URL {}", transferItem.getNbn(), landingPageUrl, e);
        }
    }
}
