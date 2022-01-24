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

import io.dropwizard.lifecycle.Managed;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.hibernate.SessionFactory;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

public class ConfirmArchiveTaskManager implements Managed {
    private static final Logger log = LoggerFactory.getLogger(ConfirmArchiveTaskManager.class);

    private final Scheduler scheduler;
    private final String schedule;

    private final SessionFactory sessionFactory;
    private final TransferItemDAO transferItemDAO;
    private final String workingDir;
    private final String dataArchiveRoot;
    private final ExecutorService executorService;

    public ConfirmArchiveTaskManager(String schedule, SessionFactory sessionFactory, TransferItemDAO transferItemDAO, String workingDir, String dataArchiveRoot,
        ExecutorService executorService) throws SchedulerException {
        this.sessionFactory = sessionFactory;
        this.transferItemDAO = transferItemDAO;
        this.workingDir = workingDir;
        this.dataArchiveRoot = dataArchiveRoot;
        this.executorService = executorService;
        this.schedule = schedule;

        SchedulerFactory sf = new StdSchedulerFactory();
        this.scheduler = sf.getScheduler();
    }

    @Override
    public void start() throws Exception {
        var jobData = new JobDataMap();

        log.info("configuring JobDataMap for cron-based tasks");

        jobData.put("sessionFactory", sessionFactory);
        jobData.put("transferItemDAO", transferItemDAO);
        jobData.put("workingDir", workingDir);
        jobData.put("dataArchiveRoot", dataArchiveRoot);
        jobData.put("executorService", executorService);

        JobDetail job = JobBuilder.newJob(ConfirmArchiveTaskCreator.class)
            .withIdentity("job", "group")
            .setJobData(jobData)
            .build();

        CronTrigger trigger = TriggerBuilder.newTrigger()
            .withIdentity("trigger", "group")
            .withSchedule(CronScheduleBuilder.cronSchedule(this.schedule))
            .build();

        log.info("scheduling ConfirmArchiveTaskCreator for schedule '{}'", this.schedule);

        this.scheduler.scheduleJob(job, trigger);
        this.scheduler.start();
    }

    @Override
    public void stop() throws Exception {
        log.info("stopping scheduler");
        this.scheduler.shutdown();
    }
}
