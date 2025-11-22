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

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.validatebagpack.client.api.ValidateCommandDto;
import nl.knaw.dans.validatebagpack.client.api.ValidationJobStatusDto;
import nl.knaw.dans.validatebagpack.client.api.ValidationResultDto;
import nl.knaw.dans.validatebagpack.client.resources.DefaultApi;

import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

@Slf4j
public class ValidateBagPackClientImpl implements ValidateBagPackClient {
    private final DefaultApi api;
    private final Duration pollInterval;

    public ValidateBagPackClientImpl(DefaultApi api) {
        this(api, Duration.ofSeconds(1));
    }

    public ValidateBagPackClientImpl(DefaultApi api, Duration pollInterval) {
        this.api = api;
        this.pollInterval = pollInterval;
    }

    @Override
    public ValidationResultDto validateBagPack(Path bagPackPath) {
        try {
            var command = new ValidateCommandDto()
                .bagLocation(bagPackPath.toAbsolutePath().toString());
            
            // Submit validation request
            log.debug("Submitting validation request for {}", bagPackPath);
            var submitResponse = api.validateBagPackWithHttpInfo(command);
            
            // Extract Location header
            var locationHeader = submitResponse.getHeaders().get("Location");
            if (locationHeader == null || locationHeader.isEmpty()) {
                throw new RuntimeException("No Location header in response");
            }
            
            String statusUrl = locationHeader.get(0);
            UUID jobId = extractJobIdFromUrl(statusUrl);
            log.debug("Validation job submitted with ID: {}", jobId);
            
            // Poll for completion
            return pollForResult(jobId);
        }
        catch (Exception e) {
            throw new RuntimeException("Error validating BagPack: " + e.getMessage(), e);
        }
    }

    private ValidationResultDto pollForResult(UUID jobId) throws Exception {
        log.debug("Polling for validation result for job {}", jobId);
        
        while (true) {
            var status = api.getValidationStatus(jobId);
            
            log.trace("Job {} status: {}", jobId, status.getStatus());
            
            switch (status.getStatus()) {
                case DONE:
                    log.debug("Validation completed successfully for job {}", jobId);
                    return status.getResult();
                    
                case FAILED:
                    String errorMsg = status.getError() != null 
                        ? status.getError() 
                        : "Unknown error";
                    log.error("Validation failed for job {}: {}", jobId, errorMsg);
                    throw new RuntimeException("Validation failed: " + errorMsg);
                    
                case PENDING:
                case RUNNING:
                    Thread.sleep(pollInterval.toMillis());
                    break;
                    
                default:
                    throw new RuntimeException("Unknown status: " + status.getStatus());
            }
        }
    }
    
    private UUID extractJobIdFromUrl(String statusUrl) {
        // Extract UUID from URL like "http://localhost:20375/validate/550e8400-..."
        String[] parts = statusUrl.split("/");
        String jobIdStr = parts[parts.length - 1];
        
        try {
            return UUID.fromString(jobIdStr);
        }
        catch (IllegalArgumentException e) {
            throw new RuntimeException("Invalid job ID in status URL: " + statusUrl, e);
        }
    }
}
