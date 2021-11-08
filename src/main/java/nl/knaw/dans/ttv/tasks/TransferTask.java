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
package nl.knaw.dans.ttv.tasks;

import io.dropwizard.servlets.tasks.Task;
import nl.knaw.dans.ttv.core.Inbox;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class TransferTask extends Task implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TransferTask.class);
    private final List<Inbox> inboxes;
    private final TransferItemDAO transferItemDAO;


    public TransferTask(List<Inbox> inboxes, TransferItemDAO transferItemDAO) {
        super(TransferTask.class.getName());
        this.inboxes = inboxes;
        this.transferItemDAO = transferItemDAO;
    }

    @Override
    public void run() {
        log.info("Running task" + this);
        for (Inbox inbox: inboxes) {
            log.info(inbox.toString());
        }
    }

    @Override
    public void execute(Map<String, List<String>> map, PrintWriter printWriter) throws Exception {
        run();
    }

    public void extractMetadata(){

    }

}
