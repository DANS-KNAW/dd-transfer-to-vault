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
package nl.knaw.dans.transfer.resources;

import lombok.RequiredArgsConstructor;
import nl.knaw.dans.transfer.api.StatusMessageDto;
import nl.knaw.dans.transfer.core.SendToVaultFlushTaskFactory;

import javax.ws.rs.core.Response;
import java.util.concurrent.ExecutorService;

@RequiredArgsConstructor
public class SendToVaultApiResource implements SendToVaultApi {
    private final ExecutorService executorService;
    private final SendToVaultFlushTaskFactory sendToVaultFlushTaskFactory;

    @Override
    public Response sendToVaultFlushPost() {
        executorService.submit(sendToVaultFlushTaskFactory.create());
        return Response.accepted(new StatusMessageDto().status("OK").message("Flush to Vault requested")).build();
    }
}
