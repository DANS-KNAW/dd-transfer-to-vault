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
import nl.knaw.dans.ttv.core.NbnRegistration;

import java.io.IOException;
import java.net.URI;

@AllArgsConstructor
@Slf4j
public class GmhClientImpl implements GmhClient {
    private final UrnnbnIdentifierApi api;

    private final URI catalogBaseUrl;

    @Override
    public void registerNbn(NbnRegistration nbnRegistration) throws FailedNbnRegistrationException {
        var landingPageUrl = catalogBaseUrl + nbnRegistration.getNbn();
        log.debug("Registering NBN {} with landing page URL {}", nbnRegistration.getNbn(), landingPageUrl);
        var nbnLocationDto = new NbnLocationsObjectDto()
            .identifier(nbnRegistration.getNbn())
            .addLocationsItem(landingPageUrl);
        try {
            api.createNbnLocations(nbnLocationDto);
        }
        catch (ApiException e) {
            throw new FailedNbnRegistrationException("Failed to register NBN " + nbnRegistration.getNbn(), e);
        }
    }
}
