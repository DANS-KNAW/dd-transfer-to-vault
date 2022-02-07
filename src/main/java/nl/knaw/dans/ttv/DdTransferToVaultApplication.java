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
import nl.knaw.dans.ttv.core.CollectTaskManager;
import nl.knaw.dans.ttv.core.ConfirmArchivedTaskManager;
import nl.knaw.dans.ttv.core.MetadataTaskManager;
import nl.knaw.dans.ttv.core.TarTaskManager;
import nl.knaw.dans.ttv.core.service.ArchiveStatusService;
import nl.knaw.dans.ttv.core.service.ArchiveStatusServiceImpl;
import nl.knaw.dans.ttv.core.service.FileServiceImpl;
import nl.knaw.dans.ttv.core.service.InboxWatcherFactoryImpl;
import nl.knaw.dans.ttv.core.service.OcflRepositoryFactory;
import nl.knaw.dans.ttv.core.service.OcflRepositoryServiceImpl;
import nl.knaw.dans.ttv.core.service.ProcessRunnerImpl;
import nl.knaw.dans.ttv.core.service.TarCommandRunnerImpl;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReaderImpl;
import nl.knaw.dans.ttv.core.service.TransferItemServiceImpl;
import nl.knaw.dans.ttv.db.TransferItem;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DdTransferToVaultApplication extends Application<DdTransferToVaultConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(DdTransferToVaultApplication.class);
    private final HibernateBundle<DdTransferToVaultConfiguration> hibernateBundle = new HibernateBundle<>(TransferItem.class) {

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
        final TransferItemDAO transferItemDAO = new TransferItemDAO(hibernateBundle.getSessionFactory());

        final var collectExecutorService = configuration.getCollect().getTaskQueue().build(environment);
        final var fileService = new FileServiceImpl();

        final var inboxWatcherFactory = new InboxWatcherFactoryImpl();

        final var transferItemService = new UnitOfWorkAwareProxyFactory(hibernateBundle).create(TransferItemServiceImpl.class, TransferItemDAO.class, transferItemDAO);

        final var metadataReader = new TransferItemMetadataReaderImpl(environment.getObjectMapper(), fileService);

        // the Collect task, which listens to new files on the network-drive shares
        final var collectTaskManager = new CollectTaskManager(
            configuration.getCollect().getInboxes(),
            configuration.getMetadata().getInbox(),
            configuration.getCollect().getPollingInterval(),
            collectExecutorService,
            transferItemService,
            metadataReader,
            fileService,
            inboxWatcherFactory
        );

        environment.lifecycle().manage(collectTaskManager);

        // the Metadata task, which analyses the zip files and stores this information in the database
        // and then moves it to the tar inbox
        final var metadataExecutorService = configuration.getMetadata().getTaskQueue().build(environment);
        final var metadataTaskManager = new MetadataTaskManager(
            configuration.getMetadata().getInbox(),
            configuration.getCreateOcflTar().getInbox(),
            configuration.getMetadata().getPollingInterval(),
            metadataExecutorService,
            transferItemService,
            metadataReader,
            fileService,
            inboxWatcherFactory
        );
        environment.lifecycle().manage(metadataTaskManager);

        // the process that looks for new files in the tar-inbox, and when reaching a certain combined size, tars them
        // and sends it to the archiving service
        final var ocflRepositoryFactory = new OcflRepositoryFactory();
        final var ocflRepositoryService = new OcflRepositoryServiceImpl(fileService, ocflRepositoryFactory);
        final var processRunner = new ProcessRunnerImpl();
        final var tarCommandRunner = new TarCommandRunnerImpl(processRunner);

        final var createTarExecutorService = configuration.getCreateOcflTar().getTaskQueue().build(environment);

        final var tarTaskManager = new TarTaskManager(
            configuration.getCreateOcflTar().getInbox(),
            configuration.getCreateOcflTar().getWorkDir(),
            configuration.getCreateOcflTar().getInboxThreshold(),
            configuration.getCreateOcflTar().getTarCommand(),
            configuration.getDataArchiveRoot(),
            configuration.getCreateOcflTar().getPollingInterval(),
            createTarExecutorService,
            inboxWatcherFactory,
            fileService,
            ocflRepositoryService,
            transferItemService,
            tarCommandRunner
        );

        environment.lifecycle().manage(tarTaskManager);

        // the process that checks the archiving service for status
        final var confirmArchiveExecutorService = configuration.getConfirmArchived().getTaskQueue().build(environment);
        final var confirmConfig = configuration.getConfirmArchived();

        final ArchiveStatusService archiveStatusService = new ArchiveStatusServiceImpl(processRunner, configuration.getDataArchiveRoot());

        final var confirmArchiveTaskManager = new ConfirmArchivedTaskManager(confirmConfig.getCron(), configuration.getCreateOcflTar().getWorkDir(), configuration.getDataArchiveRoot(),
            confirmArchiveExecutorService, transferItemService, archiveStatusService, ocflRepositoryService);

        environment.lifecycle().manage(confirmArchiveTaskManager);
    }

}
