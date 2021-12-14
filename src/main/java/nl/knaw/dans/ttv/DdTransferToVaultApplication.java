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

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.FlatOmitPrefixLayoutConfig;
import io.dropwizard.Application;
import io.dropwizard.db.PooledDataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import nl.knaw.dans.ttv.core.Inbox;
import nl.knaw.dans.ttv.core.InboxWatcher;
import nl.knaw.dans.ttv.core.Task;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
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

    private final HibernateBundle<DdTransferToVaultConfiguration> hibernateBundle = new HibernateBundle<>(TransferItem.class) {
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
        final ExecutorService executorService = configuration.getTaskQueue().build(environment);
        List<Inbox> inboxes = new ArrayList<>();
        Map<String,String> outboxes = configuration.getOutboxes();
        List<InboxWatcher> inboxWatchers = new ArrayList<>();

        //TODO initialize the OCFL repo
        Inbox.TRANSFER_OUTBOX = Path.of(outboxes.get("transferOutboxPath"));
        Inbox.OCFL_STAGING_DIR = Path.of(outboxes.get("ocflWorkPath"));

        OcflRepository ocflRepository = new OcflRepositoryBuilder()
                .defaultLayoutConfig(new FlatOmitPrefixLayoutConfig().setDelimiter("urn:uuid:"))
                .storage(ocflStorageBuilder -> ocflStorageBuilder.fileSystem(Inbox.TRANSFER_OUTBOX))
                .workDir(Inbox.OCFL_STAGING_DIR)
                .build();

        for (Map<String, String> inbox : configuration.getInboxes()) {
            Inbox newInbox = new UnitOfWorkAwareProxyFactory(hibernateBundle)
                    .create(Inbox.class,
                            new Class[] {String.class, Path.class, TransferItemDAO.class, OcflRepository.class, SessionFactory.class, ObjectMapper.class},
                            new Object[] {inbox.get("name"), Paths.get(inbox.get("path")), transferItemDAO, ocflRepository, hibernateBundle.getSessionFactory(), environment.getObjectMapper()});
            inboxes.add(newInbox);
            try {
                inboxWatchers.add(new InboxWatcher(newInbox, executorService));
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                throw new RuntimeException("InboxWatcher failed to start");
            }
        }

        List<Task> tasks = new ArrayList<>();
        for (Inbox inbox: inboxes) {
            tasks.addAll(inbox.createTransferItemTasks());
        }
        tasks.sort(Inbox.TASK_QUEUE_DATE_COMPARATOR);

        tasks.forEach(executorService::execute);
        inboxWatchers.forEach(inboxWatcher -> environment.lifecycle().manage(inboxWatcher));
    }

}
