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

package nl.knaw.dans.ttv;

import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.db.PooledDataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import nl.knaw.dans.lib.util.ClientProxyBuilder;
import nl.knaw.dans.ttv.client.GmhClient;
import nl.knaw.dans.ttv.client.GmhClientImpl;
import nl.knaw.dans.ttv.client.VaultCatalogClient;
import nl.knaw.dans.ttv.client.VaultCatalogClientImpl;
import nl.knaw.dans.ttv.config.DdTransferToVaultConfig;
import nl.knaw.dans.ttv.core.CollectTaskManager;
import nl.knaw.dans.ttv.core.ExtractMetadataTaskManager;
import nl.knaw.dans.ttv.core.SendToVaultTaskManager;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.ttv.core.oaiore.OaiOreMetadataReader;
import nl.knaw.dans.ttv.core.service.FileServiceImpl;
import nl.knaw.dans.ttv.core.service.InboxWatcherFactoryImpl;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReaderImpl;
import nl.knaw.dans.ttv.core.service.TransferItemServiceImpl;
import nl.knaw.dans.ttv.core.service.TransferItemValidatorImpl;
import nl.knaw.dans.ttv.db.TransferItemDao;
import nl.knaw.dans.ttv.health.FilesystemHealthCheck;
import nl.knaw.dans.ttv.health.InboxHealthCheck;
import nl.knaw.dans.ttv.health.PartitionHealthCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DdTransferToVaultApplication extends Application<DdTransferToVaultConfig> {

    private static final Logger log = LoggerFactory.getLogger(DdTransferToVaultApplication.class);
    private final HibernateBundle<DdTransferToVaultConfig> hibernateBundle = new HibernateBundle<>(TransferItem.class) {

        @Override
        public PooledDataSourceFactory getDataSourceFactory(DdTransferToVaultConfig configuration) {
            return configuration.getDatabase();
        }
    };

    public static void main(final String[] args) throws Exception {
        new DdTransferToVaultApplication().run(args);
    }

    @Override
    public String getName() {
        return "DD Transfer To Vault";
    }

    @Override
    public void initialize(final Bootstrap<DdTransferToVaultConfig> bootstrap) {
        bootstrap.addBundle(hibernateBundle);
    }

    @Override
    public void run(final DdTransferToVaultConfig configuration, final Environment environment) {
        final var transferItemDao = new TransferItemDao(hibernateBundle.getSessionFactory());

        final var transferItemValidator = new TransferItemValidatorImpl();
        final var collectExecutorService = configuration.getCollect().getTaskQueue().build(environment);
        final var fileService = new FileServiceImpl();

        final var inboxWatcherFactory = new InboxWatcherFactoryImpl();

        final var transferItemService = new UnitOfWorkAwareProxyFactory(hibernateBundle)
            .create(TransferItemServiceImpl.class, new Class[] { TransferItemDao.class },
                new Object[] { transferItemDao });

        final var oaiOreMetadataReader = new OaiOreMetadataReader();
        final var metadataReader = new TransferItemMetadataReaderImpl(fileService, oaiOreMetadataReader);

        environment.healthChecks().register("Inbox", new InboxHealthCheck(configuration, fileService));
        environment.healthChecks().register("Filesystem", new FilesystemHealthCheck(configuration, fileService));
        environment.healthChecks().register("Partitions", new PartitionHealthCheck(configuration, fileService));

        log.info("Creating CollectTaskManager");
        final var collectTaskManager = new CollectTaskManager(
            configuration.getCollect().getInboxes(),
            configuration.getExtractMetadata().getInbox(),
            configuration.getCollect().getPollingInterval(),
            collectExecutorService,
            transferItemService, metadataReader, fileService, inboxWatcherFactory);

        environment.lifecycle().manage(collectTaskManager);

        log.info("Creating ExtractMetadataTaskManager");
        final var vaultCatalogClient = createVaultCatalogClient(configuration);
        final var gmhClient = createGmhClient(configuration);
        final var extractMetadataExecutorService = configuration.getExtractMetadata().getTaskQueue().build(environment);
        final var extractMetadataTaskManager = new ExtractMetadataTaskManager(configuration.getExtractMetadata().getInbox(), configuration.getSendToVault().getInbox(),
            configuration.getExtractMetadata().getPollingInterval(), extractMetadataExecutorService, transferItemService, metadataReader, fileService, inboxWatcherFactory, transferItemValidator,
            vaultCatalogClient, gmhClient);
        environment.lifecycle().manage(extractMetadataTaskManager);

        log.info("Creating SendToVaultTaskManager");
        final var dataVaultProxy = createDataVaultProxy(configuration);
        final var sendToVaultTaskManager = new SendToVaultTaskManager(
            configuration.getSendToVault().getInbox(),
            configuration.getSendToVault().getOutbox(),
            configuration.getSendToVault().getPollingInterval(),
            fileService,
            transferItemService,
            metadataReader,
            configuration.getSendToVault().getWork(),
            configuration.getSendToVault().getMaxBatchSize(),
            dataVaultProxy,
            inboxWatcherFactory);
        environment.lifecycle().manage(sendToVaultTaskManager);
    }

    private VaultCatalogClient createVaultCatalogClient(DdTransferToVaultConfig configuration) {
        final var vaultCatalogProxy = new ClientProxyBuilder<nl.knaw.dans.vaultcatalog.client.invoker.ApiClient, nl.knaw.dans.vaultcatalog.client.resources.DefaultApi>()
            .apiClient(new nl.knaw.dans.vaultcatalog.client.invoker.ApiClient())
            .basePath(configuration.getVaultCatalog().getUrl())
            .httpClient(configuration.getVaultCatalog().getHttpClient())
            .defaultApiCtor(nl.knaw.dans.vaultcatalog.client.resources.DefaultApi::new)
            .build();
        return new VaultCatalogClientImpl(vaultCatalogProxy);
    }

    private GmhClient createGmhClient(DdTransferToVaultConfig configuration) {
        final var gmhProxy = new ClientProxyBuilder<nl.knaw.dans.gmh.client.invoker.ApiClient, nl.knaw.dans.gmh.client.resources.UrnnbnIdentifierApi>()
            .apiClient(new nl.knaw.dans.gmh.client.invoker.ApiClient().setBearerToken(configuration.getExtractMetadata().getGmh().getToken()))
            .basePath(configuration.getExtractMetadata().getGmh().getUrl())
            .httpClient(configuration.getExtractMetadata().getGmh().getHttpClient())
            .defaultApiCtor(nl.knaw.dans.gmh.client.resources.UrnnbnIdentifierApi::new)
            .build();
        return new GmhClientImpl(gmhProxy, configuration.getExtractMetadata().getCatalogBaseUrl());
    }

    private nl.knaw.dans.datavault.client.resources.DefaultApi createDataVaultProxy(DdTransferToVaultConfig configuration) {
        return new ClientProxyBuilder<nl.knaw.dans.datavault.client.invoker.ApiClient, nl.knaw.dans.datavault.client.resources.DefaultApi>()
            .apiClient(new nl.knaw.dans.datavault.client.invoker.ApiClient())
            .basePath(configuration.getDataVault().getUrl())
            .httpClient(configuration.getDataVault().getHttpClient())
            .defaultApiCtor(nl.knaw.dans.datavault.client.resources.DefaultApi::new)
            .build();
    }
}
