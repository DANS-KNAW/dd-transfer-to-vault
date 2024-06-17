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

@AllArgsConstructor
@Slf4j
public class GmhClientImpl implements GmhClient {
    private final UrnnbnIdentifierApi api;
    @Override
    public void registerNbn(NbnRegistration nbnRegistration) throws FailedNbnRegistrationException {
        log.debug("Registering NBN {} with landing page URL {}", nbnRegistration.getNbn(), nbnRegistration.getLocation());
        var nbnLocationDto = new NbnLocationsObjectDto()
            .identifier(nbnRegistration.getNbn())
            .addLocationsItem(nbnRegistration.getLocation().toString());
        try {
            api.createNbnLocations(nbnLocationDto);
        }
        catch (ApiException e) {
            throw new FailedNbnRegistrationException("Failed to register NBN " + nbnRegistration.getNbn(), e);
        }
    }
}
