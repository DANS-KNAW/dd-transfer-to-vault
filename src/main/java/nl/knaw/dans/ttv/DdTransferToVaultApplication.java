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
import nl.knaw.dans.ttv.core.Inbox;
import nl.knaw.dans.ttv.core.Task;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.ttv.core.TransferTask;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class DdTransferToVaultApplication extends Application<DdTransferToVaultConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(DdTransferToVaultApplication.class);

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
        final ExecutorService executorService = configuration.getJobQueue().build(environment);
        List<Inbox> inboxes = new ArrayList<>();

        //Initialize inboxes
        for (Map<String, String> inbox : configuration.getInboxes()) {
            Inbox newInbox = new UnitOfWorkAwareProxyFactory(hibernateBundle)
                    .create(Inbox.class, new Class[] {String.class, Path.class}, new Object[] {inbox.get("name"), Paths.get(inbox.get("path"))});
            inboxes.add(newInbox);
        }

        //get a list of sorted(creationTime) TransferTasks containing TransferItems, which have been checked for consistency disk/db
        List<Task> tasks = new ArrayList<>();
        for (Inbox inbox: inboxes) {
            inbox.setSessionFactory(hibernateBundle.getSessionFactory());
            inbox.setTransferItemDAO(transferItemDAO);
            tasks.addAll(inbox.createTransferItemTasks());
        }
        tasks.sort(Inbox.TASK_QUEUE_DATE_COMPARATOR);

        tasks.forEach(executorService::execute);
    }

}
