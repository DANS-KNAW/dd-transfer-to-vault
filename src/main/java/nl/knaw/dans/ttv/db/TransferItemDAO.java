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
import nl.knaw.dans.ttv.core.TransferItem;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class TransferItemDAO extends AbstractDAO<TransferItem> {

    private static final Logger log = LoggerFactory.getLogger(TransferItemDAO.class);

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
        return list(namedTypedQuery(TransferItem.TRANSFER_ITEM_FIND_ALL));
    }

    public List<TransferItem> findAllStatusExtract() {
        return list(namedTypedQuery(TransferItem.TRANSFER_ITEM_FIND_ALL_STATUS_EXTRACT));
    }

    public List<TransferItem> findAllStatusMove() {
        return list(namedTypedQuery(TransferItem.TRANSFER_ITEM_FIND_ALL_STATUS_MOVE));
    }

    public List<TransferItem> findAllStatusTar() {
        return list(namedTypedQuery(TransferItem.TRANSFER_ITEM_FIND_ALL_STATUS_TAR));
    }

    public List<TransferItem> findAllStatusTarring() {
        return list(namedTypedQuery(TransferItem.TRANSFER_ITEM_FIND_ALL_STATUS_TARRING));
    }

    public void merge(TransferItem transferItem) {
        currentSession().merge(transferItem);
    }

    public void flush() {
        currentSession().flush();
    }

    public TransferItem findByDvePath(String path) {
        var query = currentSession().createQuery("from TransferItem where dveFilePath = :path", TransferItem.class);
        query.setParameter("path", path);

        return query.getSingleResult();
    }
}
