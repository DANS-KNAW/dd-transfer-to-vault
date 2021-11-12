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

import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import nl.knaw.dans.ttv.core.Inbox;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@ExtendWith(DropwizardExtensionsSupport.class)
public class TestFixture {

    protected static TransferItemDAO TRANSFER_ITEM_DAO;
    protected static List<Inbox> INBOXES = Collections.singletonList(new Inbox("DvInstance1", Paths.get("src/test/resources/data/DvInstance1")));
    protected static DAOTestExtension DAO_TEST_RULE;

    @BeforeAll
    static void initAll() {
        DAO_TEST_RULE = DAOTestExtension.newBuilder()
                .addEntityClass(TransferItem.class)
                .setDriver("org.hsqldb.jdbcDriver")
                .setUrl("jdbc:hsqldb:mem" + UUID.randomUUID())
                .build();
    }


}
