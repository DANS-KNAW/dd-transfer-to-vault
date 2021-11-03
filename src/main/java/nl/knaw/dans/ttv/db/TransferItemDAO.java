package nl.knaw.dans.ttv.db;

import io.dropwizard.hibernate.AbstractDAO;
import nl.knaw.dans.ttv.core.TransferItem;
import org.hibernate.SessionFactory;

public class TransferItemDAO extends AbstractDAO<TransferItem> {

    public TransferItemDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }
}
