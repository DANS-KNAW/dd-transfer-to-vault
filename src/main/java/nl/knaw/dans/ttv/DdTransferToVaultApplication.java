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
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import nl.knaw.dans.ttv.core.Inbox;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import nl.knaw.dans.ttv.jobs.TransferJob;
import org.eclipse.jetty.server.Server;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;

public class DdTransferToVaultApplication extends Application<DdTransferToVaultConfiguration> {

    public static void main(final String[] args) throws Exception {
        new DdTransferToVaultApplication().run(args);
    }

    private final HibernateBundle<DdTransferToVaultConfiguration> hibernateBundle = new HibernateBundle<DdTransferToVaultConfiguration>(TransferItem.class) {
        @Override
        public PooledDataSourceFactory getDataSourceFactory(DdTransferToVaultConfiguration configuration) {
            return configuration.getDataSourceFactory();
        }
    };

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
        final List<Inbox> inboxes = configuration.buildInboxes();
        final TransferJob transferJob = new TransferJob(inboxes, transferItemDAO);
        final DefaultServerFactory server = configuration.getDefaultServerFactory();

        ExecutorService executorService = environment.lifecycle()
                .executorService(DdTransferToVaultApplication.class.getName())
                .maxThreads(server.getMaxThreads())
                .minThreads(server.getMinThreads())
                .workQueue(new LinkedBlockingDeque<>(server.getMaxQueuedRequests()))
                .build();

        executorService.execute(transferJob);
    }

}
