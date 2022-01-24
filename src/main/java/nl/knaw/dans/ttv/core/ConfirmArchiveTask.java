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
package nl.knaw.dans.ttv.core;

import io.dropwizard.hibernate.UnitOfWork;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfirmArchiveTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ConfirmArchiveTask.class);

    private final TransferItemDAO transferItemDAO;
    private final String workingDir;
    private final String dataArchiveRoot;

    public ConfirmArchiveTask(TransferItemDAO transferItemDAO, String workingDir, String dataArchiveRoot) {
        this.transferItemDAO = transferItemDAO;
        this.workingDir = workingDir;
        this.dataArchiveRoot = dataArchiveRoot;
    }

    @Override
    public void run() {

//        getRepos();
    }

    @UnitOfWork
    public void getRepos() {
        var items = transferItemDAO.findAllStatusTarring();

        for (var item: items) {
            System.out.println(item.getDveFilePath());
        }
    }

    @Override
    public String toString() {
        return "ConfirmArchiveTask{" +
            "workingDir='" + workingDir + '\'' +
            ", dataArchiveRoot='" + dataArchiveRoot + '\'' +
            '}';
    }
}
