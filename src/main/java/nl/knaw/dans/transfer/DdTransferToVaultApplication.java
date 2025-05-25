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
import nl.knaw.dans.lib.util.ClientProxyBuilder;
import nl.knaw.dans.lib.util.inbox.Inbox;
import nl.knaw.dans.transfer.client.DataVaultClient;
import nl.knaw.dans.transfer.client.GmhClient;
import nl.knaw.dans.transfer.client.GmhClientImpl;
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
import nl.knaw.dans.transfer.core.NbnRegistrationTaskFactory;
import nl.knaw.dans.transfer.core.PropertiesFileFilter;
import nl.knaw.dans.transfer.core.RemoveEmptyTargetDirsTask;
import nl.knaw.dans.transfer.core.RemoveXmlFilesTask;
import nl.knaw.dans.transfer.core.SendToVaultTaskFactory;
import nl.knaw.dans.transfer.core.SequencedTasks;
import nl.knaw.dans.transfer.core.oaiore.OaiOreMetadataReader;
import nl.knaw.dans.vaultcatalog.client.invoker.ApiClient;
import nl.knaw.dans.vaultcatalog.client.resources.DefaultApi;
import org.apache.commons.io.filefilter.FileFilterUtils;

import java.util.concurrent.CountDownLatch;

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
        environment.lifecycle().manage(Inbox.builder()
            .fileFilter(new DveFileFilter())
            .inbox(configuration.getTransfer().getSendToVault().getInbox().getPath())
            .taskFactory(SendToVaultTaskFactory.builder()
                .currentBatchWorkDir(configuration.getTransfer().getSendToVault().getDataVault().getCurrentBatchWorkingDir())
                .batchThreshold(configuration.getTransfer().getSendToVault().getDataVault().getBatchThreshold())
                .layerThreshold(configuration.getTransfer().getSendToVault().getDataVault().getLayerThreshold())
                .outboxProcessed(configuration.getTransfer().getSendToVault().getOutbox().getProcessed())
                .outboxFailed(configuration.getTransfer().getSendToVault().getOutbox().getFailed())
                .dataVaultBatchRoot(configuration.getTransfer().getSendToVault().getDataVault().getBatchRoot())
                .dataVaultClient(createDataVaultClient(configuration))
                .build())
            .build());

        VaultCatalogClient vaultCatalogClient = createVaultCatalogClient(configuration);
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
                        .dveMetadataReader(new DveMetadataReader(
                            fileService,
                            new OaiOreMetadataReader(),
                            new DataFileMetadataReader(fileService)))
                        .vaultCatalogClient(vaultCatalogClient).build())
                .inbox(configuration.getTransfer().getExtractMetadata().getInbox().getPath())
                .executorService(configuration.getTransfer().getExtractMetadata().getTaskQueue().build(environment))
                .interval(Math.toIntExact(configuration.getTransfer().getExtractMetadata().getInbox().getPollingInterval().toMilliseconds()))
                .inboxItemComparator(CreationTimeComparator.getInstance())
                .build());

        environment.lifecycle().manage(
            Inbox.builder()
                .awaitLatch(startCollectInbox)
                .onPollingHandler(new SequencedTasks(
                    new RemoveEmptyTargetDirsTask(configuration.getTransfer().getCollectDve().getOutbox().getProcessed()),
                    new RemoveXmlFilesTask(configuration.getTransfer().getCollectDve().getInbox().getPath())))
                .fileFilter(new DveFileFilter())
                .taskFactory(
                    CollectDveTaskFactory.builder()
                        .nbnSource(configuration.getTransfer().getCollectDve().getNbnSource())
                        .destinationRoot(configuration.getTransfer().getCollectDve().getOutbox().getProcessed())
                        .failedOutbox(configuration.getTransfer().getCollectDve().getOutbox().getFailed()).build())
                .inbox(configuration.getTransfer().getCollectDve().getInbox().getPath())
                // N.B. this MUST be a single-threaded executor to prevent DVEs from out-racing each other via parallel processing, which would mess up the order of the DVEs.
                .executorService(environment.lifecycle().executorService("transfer-inbox").maxThreads(1).minThreads(1).build())
                .interval(Math.toIntExact(configuration.getTransfer().getCollectDve().getInbox().getPollingInterval().toMilliseconds()))
                .inboxItemComparator(CreationTimeComparator.getInstance())
                .build());

        environment.lifecycle().manage(
            Inbox.builder()
                .inbox(configuration.getNbnRegistration().getInbox().getPath())
                .interval(Math.toIntExact(configuration.getNbnRegistration().getInbox().getPollingInterval().toMilliseconds()))
                .executorService(environment.lifecycle().executorService("nbn-registration-inbox").maxThreads(1).minThreads(1).build())
                .inboxItemComparator(CreationTimeComparator.getInstance())
                .fileFilter(new PropertiesFileFilter())
                .taskFactory(NbnRegistrationTaskFactory.builder()
                    .gmhClient(createGmhClient(configuration))
                    .outboxProcessed(configuration.getNbnRegistration().getOutbox().getProcessed())
                    .outboxFailed(configuration.getNbnRegistration().getOutbox().getFailed())
                    .build())
                .build());

    }

    private VaultCatalogClient createVaultCatalogClient(DdTransferToVaultConfiguration configuration) {
        final var vaultCatalogProxy = new ClientProxyBuilder<ApiClient, DefaultApi>()
            .apiClient(new nl.knaw.dans.vaultcatalog.client.invoker.ApiClient())
            .basePath(configuration.getVaultCatalog().getUrl())
            .httpClient(configuration.getVaultCatalog().getHttpClient())
            .defaultApiCtor(nl.knaw.dans.vaultcatalog.client.resources.DefaultApi::new)
            .build();
        return new VaultCatalogClientImpl(vaultCatalogProxy);
    }

    private DataVaultClient createDataVaultClient(DdTransferToVaultConfiguration configuration) {
        return new DataVaultClient(new ClientProxyBuilder<nl.knaw.dans.datavault.client.invoker.ApiClient, nl.knaw.dans.datavault.client.resources.DefaultApi>()
            .apiClient(new nl.knaw.dans.datavault.client.invoker.ApiClient())
            .basePath(configuration.getDataVault().getUrl())
            .httpClient(configuration.getDataVault().getHttpClient())
            .defaultApiCtor(nl.knaw.dans.datavault.client.resources.DefaultApi::new)
            .build());
    }

    private GmhClient createGmhClient(DdTransferToVaultConfiguration configuration) {
        return new GmhClientImpl(new ClientProxyBuilder<nl.knaw.dans.gmh.client.invoker.ApiClient, nl.knaw.dans.gmh.client.resources.UrnnbnIdentifierApi>()
            .apiClient(new nl.knaw.dans.gmh.client.invoker.ApiClient().setBearerToken(configuration.getNbnRegistration().getGmh().getToken()))
            .basePath(configuration.getNbnRegistration().getGmh().getUrl())
            .httpClient(configuration.getNbnRegistration().getGmh().getHttpClient())
            .defaultApiCtor(nl.knaw.dans.gmh.client.resources.UrnnbnIdentifierApi::new)
            .build());
    }

}
