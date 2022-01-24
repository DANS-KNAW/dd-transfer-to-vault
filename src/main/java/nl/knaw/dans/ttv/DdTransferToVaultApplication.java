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
import nl.knaw.dans.ttv.core.ConfirmArchiveTaskManager;
import nl.knaw.dans.ttv.core.OcflRepositoryFactory;
import nl.knaw.dans.ttv.core.TarTaskManager;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

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
        final var createOcflExecutorService = configuration.getCreateOcfl().getTaskQueue().build(environment);

        // the Collect task, which listens to new files on the network-drive shares
        var collectTaskManager = new CollectTaskManager(configuration.getCollect().getInboxes(), configuration.getCreateTar().getInbox(), hibernateBundle.getSessionFactory(), collectExecutorService,
            environment.getObjectMapper(), transferItemDAO);

        environment.lifecycle().manage(collectTaskManager);

        // the process that looks for new files in the tar-inbox, and when reaching a certain combined size, tars them
        // and sends it to the archiving service
        final var ocflRepositoryFactory = new OcflRepositoryFactory();
        final var createTarExecutorService = configuration.getCreateTar().getTaskQueue().build(environment);

        var tarTaskManager = new UnitOfWorkAwareProxyFactory(hibernateBundle).create(TarTaskManager.class,
            new Class[] { String.class, String.class, long.class, String.class, String.class, TransferItemDAO.class, SessionFactory.class, ExecutorService.class, OcflRepositoryFactory.class },
            new Object[] { configuration.getCreateTar().getInbox(), configuration.getCreateTar().getWorkDir(), configuration.getCreateTar().getInboxThreshold(),
                configuration.getCreateTar().getTarCommand(), configuration.getCreateTar().getDataArchiveRoot(), transferItemDAO, hibernateBundle.getSessionFactory(), createTarExecutorService,
                ocflRepositoryFactory });

        environment.lifecycle().manage(tarTaskManager);

        // the process that checks the archiving service for status
        final var confirmArchiveExecutorService = configuration.getConfirmArchived().getTaskQueue().build(environment);
        final var confirmConfig = configuration.getConfirmArchived();

        var confirmArchiveTaskManager = new UnitOfWorkAwareProxyFactory(hibernateBundle).create(ConfirmArchiveTaskManager.class,
            new Class[] { String.class, SessionFactory.class, TransferItemDAO.class, String.class, String.class, ExecutorService.class },
            new Object[] { confirmConfig.getCron(), hibernateBundle.getSessionFactory(), transferItemDAO, configuration.getCreateTar().getWorkDir(), confirmConfig.getDataArchiveRoot(),
                confirmArchiveExecutorService });

        environment.lifecycle().manage(confirmArchiveTaskManager);
    }

}
