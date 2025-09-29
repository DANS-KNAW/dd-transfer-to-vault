/*
 * Copyright (C) 2025 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.transfer.client;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.gmh.client.api.NbnLocationsObjectDto;
import nl.knaw.dans.gmh.client.invoker.ApiException;
import nl.knaw.dans.gmh.client.resources.UrnNbnIdentifierApi;
import nl.knaw.dans.transfer.core.RegistrationToken;

@AllArgsConstructor
@Slf4j
public class GmhClientImpl implements GmhClient {
    private final UrnNbnIdentifierApi api;
    @Override
    public void registerNbn(RegistrationToken registrationToken) throws FailedNbnRegistrationException {
        log.debug("Registering NBN {} with landing page URL {}", registrationToken.getNbn(), registrationToken.getLocation());
        var nbnLocationDto = new NbnLocationsObjectDto()
            .identifier(registrationToken.getNbn())
            .addLocationsItem(registrationToken.getLocation().toString());
        try {
            api.createNbnLocations(nbnLocationDto);
        }
        catch (ApiException e) {
            throw new FailedNbnRegistrationException("Failed to register NBN " + registrationToken.getNbn(), e);
        }
    }
}
