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

import io.dropwizard.Application;
import io.dropwizard.db.PooledDataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import nl.knaw.dans.ttv.client.OcflObjectVersionApi;
import nl.knaw.dans.ttv.client.TarApi;
import nl.knaw.dans.ttv.client.VaultCatalogAPIRepository;
import nl.knaw.dans.ttv.core.*;
import nl.knaw.dans.ttv.core.service.*;
import nl.knaw.dans.ttv.db.*;
import nl.knaw.dans.ttv.health.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DdTransferToVaultApplication extends Application<DdTransferToVaultConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(DdTransferToVaultApplication.class);
    private final HibernateBundle<DdTransferToVaultConfiguration> hibernateBundle = new HibernateBundle<>(TransferItem.class, Tar.class, TarPart.class) {

        @Override
        public PooledDataSourceFactory getDataSourceFactory(DdTransferToVaultConfiguration configuration) {
            return configuration.getDataSourceFactory();
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
    public void initialize(final Bootstrap<DdTransferToVaultConfiguration> bootstrap) {
        bootstrap.addBundle(hibernateBundle);
    }

    @Override
    public void run(final DdTransferToVaultConfiguration configuration, final Environment environment) {
        log.info("Creating required objects");
        final var transferItemDAO = new TransferItemDAO(hibernateBundle.getSessionFactory());
        final var tarDAO = new TarDAO(hibernateBundle.getSessionFactory());

        final var transferItemValidator = new TransferItemValidatorImpl();
        final var collectExecutorService = configuration.getCollect().getTaskQueue().build(environment);
        final var fileService = new FileServiceImpl();

        final var inboxWatcherFactory = new InboxWatcherFactoryImpl();

        final var transferItemService = new UnitOfWorkAwareProxyFactory(hibernateBundle).create(TransferItemServiceImpl.class, new Class[]{TransferItemDAO.class, TarDAO.class},
            new Object[]{transferItemDAO, tarDAO});

        final var metadataReader = new TransferItemMetadataReaderImpl(environment.getObjectMapper(), fileService);
        // the process that looks for new files in the tar-inbox, and when reaching a certain combined size, tars them
        // and sends it to the archiving service
        final var ocflRepositoryFactory = new OcflRepositoryFactory();
        final var ocflRepositoryService = new OcflRepositoryServiceImpl(fileService, ocflRepositoryFactory);
        final var processRunner = new ProcessRunnerImpl();
        final var tarCommandRunner = new TarCommandRunnerImpl(configuration.getDataArchive(), processRunner);
        final var archiveMetadataService = new ArchiveMetadataServiceImpl(configuration.getDataArchive(), processRunner);
        final var createTarExecutorService = configuration.getCreateOcflTar().getTaskQueue().build(environment);

        environment.healthChecks().register("Inbox", new InboxHealthCheck(configuration, fileService));
        environment.healthChecks().register("Filesystem", new FilesystemHealthCheck(configuration, fileService));
        environment.healthChecks().register("Partitions", new PartitionHealthCheck(configuration, fileService));
        environment.healthChecks().register("DmftarLocal", new LocalDmftarHealthCheck(configuration, processRunner));
        environment.healthChecks().register("DmftarRemote", new RemoteDmftarHealthCheck(configuration, tarCommandRunner));
        environment.healthChecks().register("SSH", new SSHHealthCheck(tarCommandRunner));

        final var vaultCatalogRepository = buildCatalogRepository(configuration.getConfirmArchived().getVaultServiceEndpoint());


        // the Collect task, which listens to new files on the network-drive shares
        log.info("Creating CollectTaskManager");
        final var collectTaskManager = new CollectTaskManager(configuration.getCollect().getInboxes(), configuration.getExtractMetadata().getInbox(), configuration.getCollect().getPollingInterval(),
            collectExecutorService, transferItemService, metadataReader, fileService, inboxWatcherFactory);

        environment.lifecycle().manage(collectTaskManager);

        // the Metadata task, which analyses the zip files and stores this information in the database
        // and then moves it to the tar inbox
        log.info("Creating ExtractMetadataTaskManager");
        final var extractMetadataExecutorService = configuration.getExtractMetadata().getTaskQueue().build(environment);
        final var extractMetadataTaskManager = new ExtractMetadataTaskManager(configuration.getExtractMetadata().getInbox(), configuration.getCreateOcflTar().getInbox(),
            configuration.getExtractMetadata().getPollingInterval(), extractMetadataExecutorService, transferItemService, metadataReader, fileService, inboxWatcherFactory, transferItemValidator, vaultCatalogRepository);
        environment.lifecycle().manage(extractMetadataTaskManager);

        log.info("Creating TarTaskManager");
        final var ocflTarTaskManager = new OcflTarTaskManager(configuration.getCreateOcflTar().getInbox(), configuration.getCreateOcflTar().getWorkDir(), configuration.getDataArchive().getPath(),
            configuration.getCreateOcflTar().getInboxThreshold(), configuration.getCreateOcflTar().getPollingInterval(), configuration.getCreateOcflTar().getMaxRetries(),
            configuration.getCreateOcflTar().getRetryInterval(), configuration.getCreateOcflTar().getRetrySchedule(), createTarExecutorService, inboxWatcherFactory, fileService, ocflRepositoryService,
            transferItemService, tarCommandRunner, archiveMetadataService, vaultCatalogRepository);

        environment.lifecycle().manage(ocflTarTaskManager);

        // the process that checks the archiving service for status
        final var confirmArchivedExecutorService = configuration.getConfirmArchived().getTaskQueue().build(environment);
        final var confirmConfig = configuration.getConfirmArchived();

        final var archiveStatusService = new ArchiveStatusServiceImpl(configuration.getDataArchive(), processRunner);

        log.info("Creating ConfirmArchivedTaskManager");
        final var confirmArchivedTaskManager = new ConfirmArchivedTaskManager(confirmConfig.getCron(), configuration.getCreateOcflTar().getWorkDir(), confirmArchivedExecutorService,
            transferItemService, archiveStatusService, fileService, vaultCatalogRepository);

        environment.lifecycle().manage(confirmArchivedTaskManager);
    }

    VaultCatalogRepository buildCatalogRepository(String baseUrl) {
        var tarApi = new TarApi();
        tarApi.setCustomBaseUrl(baseUrl);

        var ocflObjectVersionApi = new OcflObjectVersionApi();
        ocflObjectVersionApi.setCustomBaseUrl(baseUrl);

        return new VaultCatalogAPIRepository(tarApi, ocflObjectVersionApi);
    }
}
