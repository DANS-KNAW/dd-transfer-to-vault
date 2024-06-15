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
import io.dropwizard.hibernate.UnitOfWork;
import nl.knaw.dans.ttv.core.NbnRegistration;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.util.List;

public class NbnRegistrationDao extends AbstractDAO<NbnRegistration> {

    public NbnRegistrationDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    @UnitOfWork
    public NbnRegistration save(NbnRegistration nbnRegistration) {
        return persist(nbnRegistration);
    }

    @UnitOfWork
    public List<NbnRegistration> getPendingRegistrations() {
        return getRegistrationsByStatus(NbnRegistration.Status.PENDING);
    }

    @UnitOfWork
    public List<NbnRegistration> getFailedRegistrations() {
        return getRegistrationsByStatus(NbnRegistration.Status.FAILED);
    }

    private List<NbnRegistration> getRegistrationsByStatus(NbnRegistration.Status status) {
        Session session = currentSession();
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<NbnRegistration> cq = cb.createQuery(NbnRegistration.class);
        Root<NbnRegistration> root = cq.from(NbnRegistration.class);
        cq.select(root).where(cb.equal(root.get("status"), status));
        Query<NbnRegistration> query = session.createQuery(cq);
        return query.getResultList();
    }
}