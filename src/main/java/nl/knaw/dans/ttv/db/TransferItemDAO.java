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
package nl.knaw.dans.ttv.db;

import io.dropwizard.hibernate.AbstractDAO;
import org.hibernate.SessionFactory;

import java.util.List;
import java.util.Optional;

public class TransferItemDAO extends AbstractDAO<TransferItem> {

    public TransferItemDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public Optional<TransferItem> findById(Long id) {
        return Optional.ofNullable(get(id));
    }

    public TransferItem save(TransferItem transferItem) {
        return persist(transferItem);
    }

    public List<TransferItem> findAll() {
        return currentSession().createQuery("from TransferItem order by creationTime asc", TransferItem.class).list();
    }

    public void merge(TransferItem transferItem) {
        currentSession().merge(transferItem);
    }

    public List<TransferItem> findByStatus(TransferItem.TransferStatus status) {
        var query = currentSession().createQuery("from TransferItem where transferStatus = :status", TransferItem.class);
        query.setParameter("status", status);

        return query.list();
    }

    public Optional<TransferItem> findByIdentifier(String fileIdentifier) {
        return query("from TransferItem where datasetIdentifier = :identifier")
            .setParameter("identifier", fileIdentifier)
            .uniqueResultOptional();

    }
}
