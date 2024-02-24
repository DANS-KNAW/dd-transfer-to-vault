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

import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.db.PooledDataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import nl.knaw.dans.ttv.client.VaultCatalogClientImpl;
import nl.knaw.dans.ttv.core.CollectTaskManager;
import nl.knaw.dans.ttv.core.ExtractMetadataTaskManager;
import nl.knaw.dans.ttv.core.Tar;
import nl.knaw.dans.ttv.core.TarPart;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.ttv.core.config.DdTransferToVaultConfig;
import nl.knaw.dans.ttv.core.oaiore.OaiOreMetadataReader;
import nl.knaw.dans.ttv.core.service.FileServiceImpl;
import nl.knaw.dans.ttv.core.service.InboxWatcherFactoryImpl;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReaderImpl;
import nl.knaw.dans.ttv.core.service.TransferItemServiceImpl;
import nl.knaw.dans.ttv.core.service.TransferItemValidatorImpl;
import nl.knaw.dans.ttv.db.TarDao;
import nl.knaw.dans.ttv.db.TransferItemDao;
import nl.knaw.dans.ttv.health.FilesystemHealthCheck;
import nl.knaw.dans.ttv.health.InboxHealthCheck;
import nl.knaw.dans.ttv.health.PartitionHealthCheck;
import nl.knaw.dans.vaultcatalog.client.invoker.ApiClient;
import nl.knaw.dans.vaultcatalog.client.resources.OcflObjectVersionApi;
import nl.knaw.dans.vaultcatalog.client.resources.TarApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DdTransferToVaultApplication extends Application<DdTransferToVaultConfig> {

    private static final Logger log = LoggerFactory.getLogger(DdTransferToVaultApplication.class);
    private final HibernateBundle<DdTransferToVaultConfig> hibernateBundle = new HibernateBundle<>(TransferItem.class, Tar.class, TarPart.class) {

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
        return "Dd Transfer To Vault";
    }

    @Override
    public void initialize(final Bootstrap<DdTransferToVaultConfig> bootstrap) {
        bootstrap.addBundle(hibernateBundle);
    }

    @Override
    public void run(final DdTransferToVaultConfig configuration, final Environment environment) {
        final var transferItemDao = new TransferItemDao(hibernateBundle.getSessionFactory());
        final var tarDao = new TarDao(hibernateBundle.getSessionFactory());

        final var transferItemValidator = new TransferItemValidatorImpl();
        final var collectExecutorService = configuration.getCollect().getTaskQueue().build(environment);
        final var fileService = new FileServiceImpl();

        final var inboxWatcherFactory = new InboxWatcherFactoryImpl();

        final var transferItemService = new UnitOfWorkAwareProxyFactory(hibernateBundle)
            .create(TransferItemServiceImpl.class, new Class[] { TransferItemDao.class, TarDao.class },
                new Object[] { transferItemDao, tarDao });

        final var oaiOreMetadataReader = new OaiOreMetadataReader();
        final var metadataReader = new TransferItemMetadataReaderImpl(fileService, oaiOreMetadataReader);

        environment.healthChecks().register("Inbox", new InboxHealthCheck(configuration, fileService));
        environment.healthChecks().register("Filesystem", new FilesystemHealthCheck(configuration, fileService));
        environment.healthChecks().register("Partitions", new PartitionHealthCheck(configuration, fileService));

        final var vaultCatalogRepository = buildCatalogRepository(configuration, environment);

        // the Collect task, which listens to new files on the network-drive shares
        log.info("Creating CollectTaskManager");
        final var collectTaskManager = new CollectTaskManager(configuration.getCollect().getInboxes(), configuration.getExtractMetadata().getInbox(), configuration.getCollect().getPollingInterval(),
            collectExecutorService, transferItemService, metadataReader, fileService, inboxWatcherFactory);

        environment.lifecycle().manage(collectTaskManager);

        // the Metadata task, which analyses the zip files and stores this information in the database
        // and then moves it to the tar inbox
        log.info("Creating ExtractMetadataTaskManager");
        final var extractMetadataExecutorService = configuration.getExtractMetadata().getTaskQueue().build(environment);
        // TODO: set outbox to inbox of the send to vault task
        final var extractMetadataTaskManager = new ExtractMetadataTaskManager(configuration.getExtractMetadata().getInbox(), null,
            configuration.getExtractMetadata().getPollingInterval(), extractMetadataExecutorService, transferItemService, metadataReader, fileService, inboxWatcherFactory, transferItemValidator,
            vaultCatalogRepository);
        environment.lifecycle().manage(extractMetadataTaskManager);
    }

    VaultCatalogClientImpl buildCatalogRepository(DdTransferToVaultConfig configuration, Environment environment) {
        var client = new JerseyClientBuilder(environment)
            .using(configuration.getVaultCatalog().getHttpClient())
            .build("vault-catalog");

        var apiClient = new ApiClient();
        apiClient.setHttpClient(client);
        apiClient.setBasePath(configuration.getVaultCatalog().getUrl().toString());

        var tarApi = new TarApi(apiClient);
        var ocflObjectVersionApi = new OcflObjectVersionApi(apiClient);

        return new VaultCatalogClientImpl(tarApi, ocflObjectVersionApi);
    }
}
