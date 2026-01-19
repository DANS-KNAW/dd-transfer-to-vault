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

package nl.knaw.dans.transfer;

import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.util.ClientProxyBuilder;
import nl.knaw.dans.lib.util.PingHealthCheck;
import nl.knaw.dans.lib.util.inbox.Inbox;
import nl.knaw.dans.transfer.client.DataVaultClient;
import nl.knaw.dans.transfer.client.ValidateBagPackClient;
import nl.knaw.dans.transfer.client.ValidateBagPackClientImpl;
import nl.knaw.dans.transfer.client.VaultCatalogClient;
import nl.knaw.dans.transfer.client.VaultCatalogClientImpl;
import nl.knaw.dans.transfer.config.DdTransferToVaultConfiguration;
import nl.knaw.dans.transfer.core.CollectDveTaskFactory;
import nl.knaw.dans.transfer.core.CreationTimeComparator;
import nl.knaw.dans.transfer.core.DataFileMetadataReader;
import nl.knaw.dans.transfer.core.DveFileFilter;
import nl.knaw.dans.transfer.core.DveMetadataReader;
import nl.knaw.dans.transfer.core.ExtractMetadataTaskFactory;
import nl.knaw.dans.transfer.core.FileService;
import nl.knaw.dans.transfer.core.FileServiceImpl;
import nl.knaw.dans.transfer.core.PropertiesFileFilter;
import nl.knaw.dans.transfer.core.RemoveEmptyTargetDirsTask;
import nl.knaw.dans.transfer.core.RemoveXmlFilesTask;
import nl.knaw.dans.transfer.core.SendToVaultFlushTaskFactory;
import nl.knaw.dans.transfer.core.SendToVaultTaskFactory;
import nl.knaw.dans.transfer.core.SequencedTasks;
import nl.knaw.dans.transfer.core.oaiore.OaiOreMetadataReader;
import nl.knaw.dans.transfer.health.FileSystemPermissionHealthCheck;
import nl.knaw.dans.transfer.resources.SendToVaultApiResource;
import nl.knaw.dans.vaultcatalog.client.invoker.ApiClient;
import nl.knaw.dans.vaultcatalog.client.resources.DefaultApi;
import org.apache.commons.io.filefilter.FileFilterUtils;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

@Slf4j
public class DdTransferToVaultApplication extends Application<DdTransferToVaultConfiguration> {

    public static void main(final String[] args) throws Exception {
        new DdTransferToVaultApplication().run(args);
    }

    @Override
    public String getName() {
        return "DD Transfer To Vault";
    }

    @Override
    public void initialize(final Bootstrap<DdTransferToVaultConfiguration> bootstrap) {
    }

    @Override
    public void run(final DdTransferToVaultConfiguration configuration, final Environment environment) {
        FileService fileService = new FileServiceImpl();
        var dataVaultProxy = createDataVaultProxy(configuration);
        var sendToVaultExecutorService = Executors.newSingleThreadExecutor();
        var datavaultClient = new DataVaultClient(dataVaultProxy);
        environment.lifecycle().manage(Inbox.builder()
            .executorService(sendToVaultExecutorService)
            .fileFilter(new DveFileFilter())
            .inbox(configuration.getTransfer().getSendToVault().getInbox().getPath())
            .interval(Math.toIntExact(configuration.getTransfer().getSendToVault().getInbox().getPollingInterval().toMilliseconds()))
            .taskFactory(SendToVaultTaskFactory.builder()
                .currentBatchWorkDir(configuration.getTransfer().getSendToVault().getDataVault().getCurrentBatchWorkingDir())
                .batchThreshold(configuration.getTransfer().getSendToVault().getDataVault().getBatchThreshold())
                .outboxProcessed(configuration.getTransfer().getSendToVault().getOutbox().getProcessed())
                .outboxFailed(configuration.getTransfer().getSendToVault().getOutbox().getFailed())
                .dataVaultBatchRoot(configuration.getTransfer().getSendToVault().getDataVault().getBatchRoot())
                .dataVaultClient(datavaultClient)
                .defaultMessage(configuration.getTransfer().getSendToVault().getDefaultMessage())
                .customProperties(configuration.getTransfer().getSendToVault().getCustomProperties())
                .fileService(fileService)
                .build())
            .build());

        var sendToVaultFlushTaskFactory = SendToVaultFlushTaskFactory.builder()
            .currentBatchWorkDir(configuration.getTransfer().getSendToVault().getDataVault().getCurrentBatchWorkingDir())
            .dataVaultBatchRoot(configuration.getTransfer().getSendToVault().getDataVault().getBatchRoot())
            .dataVaultClient(datavaultClient).build();
        environment.jersey().register(new SendToVaultApiResource(sendToVaultExecutorService,
            sendToVaultFlushTaskFactory));

        final var vaultCatalogProxy = createVaultCatalogProxy(configuration);
        VaultCatalogClient vaultCatalogClient = new VaultCatalogClientImpl(vaultCatalogProxy);
        var validateBagPackProxy = createValidateBagPackProxy(configuration);

        ValidateBagPackClient validateBagPackClient = new ValidateBagPackClientImpl(validateBagPackProxy, configuration.getValidateBagPack().getPollInterval().toJavaDuration());
        CountDownLatch startCollectInbox = new CountDownLatch(1);
        environment.lifecycle().manage(
            Inbox.builder()
                .onPollingHandler(new ReleaseLatch(startCollectInbox))
                .fileFilter(FileFilterUtils.directoryFileFilter())
                .taskFactory(
                    ExtractMetadataTaskFactory.builder()
                        .ocflStorageRoot(configuration.getTransfer().getOcflStorageRoot())
                        .outboxProcessed(configuration.getTransfer().getExtractMetadata().getOutbox().getProcessed())
                        .outboxFailed(configuration.getTransfer().getExtractMetadata().getOutbox().getFailed())
                        .outboxRejected(configuration.getTransfer().getExtractMetadata().getOutbox().getRejected())
                        .nbnRegistrationInbox(configuration.getNbnRegistration().getInbox().getPath())
                        .vaultCatalogBaseUri(configuration.getNbnRegistration().getCatalogBaseUrl())
                        .fileService(fileService)
                        .dveMetadataReader(new DveMetadataReader(
                            fileService,
                            new OaiOreMetadataReader(),
                            new DataFileMetadataReader(fileService)))
                        .vaultCatalogClient(vaultCatalogClient)
                        .validateBagPackClient(validateBagPackClient)
                        .build())
                .inbox(configuration.getTransfer().getExtractMetadata().getInbox().getPath())
                .executorService(configuration.getTransfer().getExtractMetadata().getTaskQueue().build(environment))
                .interval(Math.toIntExact(configuration.getTransfer().getExtractMetadata().getInbox().getPollingInterval().toMilliseconds()))
                .inboxItemComparator(CreationTimeComparator.getInstance())
                .build());

        environment.lifecycle().manage(
            Inbox.builder()
                .awaitLatch(startCollectInbox)
                .onPollingHandler(new SequencedTasks(
                    new RemoveEmptyTargetDirsTask(configuration.getTransfer().getCollectDve().getProcessed()),
                    new RemoveXmlFilesTask(configuration.getTransfer().getCollectDve().getInbox().getPath())))
                .fileFilter(new DveFileFilter())
                .taskFactory(
                    CollectDveTaskFactory.builder()
                        .destinationRoot(configuration.getTransfer().getCollectDve().getProcessed())
                        .inboxPath(configuration.getTransfer().getCollectDve().getInbox().getPath())
                        .fileService(fileService)
                        .build())
                .inbox(configuration.getTransfer().getCollectDve().getInbox().getPath())
                // N.B. this MUST be a single-threaded executor to prevent DVEs from out-racing each other via parallel processing, which would mess up the order of the DVEs.
                .executorService(environment.lifecycle().executorService("transfer-inbox").maxThreads(1).minThreads(1).build())
                .interval(Math.toIntExact(configuration.getTransfer().getCollectDve().getInbox().getPollingInterval().toMilliseconds()))
                .inboxItemComparator(CreationTimeComparator.getInstance())
                .build());

        environment.healthChecks().register("FileSystemPermissions", new FileSystemPermissionHealthCheck(
            configuration.getTransfer(),
            configuration.getNbnRegistration(),
            fileService)
        );

        var healthCheckResult = environment.healthChecks()
            .runHealthCheck("FileSystemPermissions");

        log.info("Running health check 'FileSystemPermissions'");
        if (!healthCheckResult.isHealthy()) {
            throw new IllegalStateException("Health check 'FileSystemPermissions' failed: " + healthCheckResult.getMessage());
        }
        log.info("Health check 'FileSystemPermissions' passed");

        environment.healthChecks().register("VaultCatalog", new PingHealthCheck(
            "VaultCatalog",
            vaultCatalogProxy.getApiClient().getHttpClient(),
            configuration.getVaultCatalog().getPingUrl())
        );
        environment.healthChecks().register("DataVault", new PingHealthCheck(
            "DataVault",
            dataVaultProxy.getApiClient().getHttpClient(),
            configuration.getDataVault().getPingUrl())
        );
        environment.healthChecks().register("ValidateBagPack", new PingHealthCheck(
            "ValidateBagPack",
            validateBagPackProxy.getApiClient().getHttpClient(),
            configuration.getValidateBagPack().getPingUrl())
        );
    }

    private DefaultApi createVaultCatalogProxy(DdTransferToVaultConfiguration configuration) {
        return new ClientProxyBuilder<ApiClient, DefaultApi>()
            .apiClient(new ApiClient())
            .basePath(configuration.getVaultCatalog().getUrl())
            .httpClient(configuration.getVaultCatalog().getHttpClient())
            .defaultApiCtor(DefaultApi::new)
            .build();
    }

    private nl.knaw.dans.datavault.client.resources.DefaultApi createDataVaultProxy(DdTransferToVaultConfiguration configuration) {
        return new ClientProxyBuilder<nl.knaw.dans.datavault.client.invoker.ApiClient, nl.knaw.dans.datavault.client.resources.DefaultApi>()
            .apiClient(new nl.knaw.dans.datavault.client.invoker.ApiClient())
            .basePath(configuration.getDataVault().getUrl())
            .httpClient(configuration.getDataVault().getHttpClient())
            .defaultApiCtor(nl.knaw.dans.datavault.client.resources.DefaultApi::new)
            .build();
    }

    private nl.knaw.dans.validatebagpack.client.resources.DefaultApi createValidateBagPackProxy(DdTransferToVaultConfiguration configuration) {
        return new ClientProxyBuilder<nl.knaw.dans.validatebagpack.client.invoker.ApiClient, nl.knaw.dans.validatebagpack.client.resources.DefaultApi>()
            .apiClient(new nl.knaw.dans.validatebagpack.client.invoker.ApiClient())
            .basePath(configuration.getValidateBagPack().getUrl())
            .httpClient(configuration.getValidateBagPack().getHttpClient())
            .defaultApiCtor(nl.knaw.dans.validatebagpack.client.resources.DefaultApi::new)
            .build();
    }
}
