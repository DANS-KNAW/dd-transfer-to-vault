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
import org.hibernate.Hibernate;
import org.hibernate.SessionFactory;

import java.util.List;
import java.util.Optional;

public class TarDAO extends AbstractDAO<Tar> {

    public TarDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public Optional<Tar> findById(String id) {
        return Optional.ofNullable(get(id));
    }

    public List<TarPart> findAllParts() {
        var query = currentSession().createQuery(
            "from TarPart", TarPart.class);

        return query.list();
    }

    public List<Tar> findAllTarsToBeConfirmed() {
        var query = currentSession().createQuery(
            "from Tar "
                + "where tarStatus = :status "
                + "and confirmCheckInProgress = false ", Tar.class);

        query.setParameter("status", Tar.TarStatus.OCFLTARCREATED);

        return query.list();
    }

    public Tar save(Tar tar) {
        return persist(tar);
    }

    public List<Tar> findByStatus(Tar.TarStatus status) {
        var query = currentSession().createQuery(
            "from Tar where tarStatus = :status", Tar.class);

        query.setParameter("status", status);

        return query.list();
    }

    public Tar saveWithParts(Tar tar, List<TarPart> parts) {
        Hibernate.initialize(tar.getTarParts());
        tar.getTarParts().clear();
        tar.getTarParts().addAll(parts);

        return save(tar);
    }

    public void evict(Tar tar) {
        currentSession().evict(tar);
    }

    public List<Tar> findTarsByConfirmInProgress() {
        var query = currentSession().createQuery(
            "from Tar where confirmCheckInProgress = true", Tar.class);

        return query.list();
    }

    public List<Tar> findTarsToBeRetried() {
        var query = currentSession().createQuery(
            "from Tar "
                + "where archiveInProgress = false "
                + "and tarStatus = :status", Tar.class);

        query.setParameter("status", Tar.TarStatus.TARRING);

        return query.list();

    }
}
