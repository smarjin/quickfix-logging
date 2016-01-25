package quickfix;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jeremybotha on 22/01/16.
 */
public class SelectableJdbcLogFactory extends JdbcLogFactory {
    private DataSource dataSource;

    private Map<SessionID,Log> sessionLogMap = new ConcurrentHashMap<SessionID, Log>();
    /**
     * Create a factory using session settings.
     *
     * @param settings
     */
    public SelectableJdbcLogFactory(SessionSettings settings) {
        super(settings);
    }

    @Override
    public Log create() {
        return null;
    }

    @Override
    public Log create(SessionID sessionID) {
        try {
            sessionLogMap.put(sessionID, new SelectableIncomingJdbcLog(getSettings(), sessionID, dataSource));
            return getLog(sessionID);
        } catch (Exception e) {
            throw new RuntimeError(e);
        }
    }

    public Log getLog(SessionID sessionID) {
        return sessionLogMap.get(sessionID);
    }
}
