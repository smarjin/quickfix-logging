package quickfix;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class overrides the base Quickfix JDBC Log with a class that uses custom configuration settings in the environment
 * Created by jeremybotha on 22/01/16.
 */
public class SelectableIncomingJdbcLog extends quickfix.JdbcLog {
    public static final String SETTING_LOG_MARKET_INCREMENTAL_REFRESH = "LogMarketIncrementalRefresh";
    public static final String SETTING_LOG_QUOTE_TRAFIC =  "LogQuoteTraffic";
    public static final String SETTING_LOG_ORDER_TRAFFIC = "LogOrderTraffic";

    private AtomicBoolean logMarketIncrementalRefreshMessages = new AtomicBoolean();
    private AtomicBoolean logQuoteTraffic = new AtomicBoolean();
    private AtomicBoolean logOrderTraffic = new AtomicBoolean();

    public SelectableIncomingJdbcLog(SessionSettings settings, SessionID sessionID, DataSource ds) throws SQLException, ClassNotFoundException, ConfigError, FieldConvertError {
        super(settings, sessionID, ds);

        if(settings.isSetting(sessionID, SETTING_LOG_MARKET_INCREMENTAL_REFRESH)) {
            logMarketIncrementalRefreshMessages.set(settings.getBool(sessionID, SETTING_LOG_MARKET_INCREMENTAL_REFRESH));
        } else {
            logMarketIncrementalRefreshMessages.set(true);
        }

        if(settings.isSetting(sessionID, SETTING_LOG_QUOTE_TRAFIC)) {
            logQuoteTraffic.set(settings.getBool(sessionID, SETTING_LOG_QUOTE_TRAFIC));
        } else {
            logQuoteTraffic.set(true);
        }

        if(settings.isSetting(sessionID, SETTING_LOG_ORDER_TRAFFIC)) {
            logOrderTraffic.set(settings.getBool(sessionID, SETTING_LOG_ORDER_TRAFFIC));
        } else {
            logOrderTraffic.set(true);
        }
    }

    /*
    *   Set custom configuration during runtime that will enable or disable FIX message logging for the classified message types
     *   below.  This is to permit on-the-fly enabling of message capture in the event we need to perform analysis or system
     *   introspection on a running production system.
    */
    public void setCustomLogConfiguration(Map<String,String>overrides) {
        logMarketIncrementalRefreshMessages.set(Boolean.valueOf(
                overrides.containsKey(SETTING_LOG_MARKET_INCREMENTAL_REFRESH) ? overrides.get(SETTING_LOG_MARKET_INCREMENTAL_REFRESH) : "true"));
        logQuoteTraffic.set(Boolean.valueOf(
                overrides.containsKey(SETTING_LOG_QUOTE_TRAFIC) ? overrides.get(SETTING_LOG_QUOTE_TRAFIC) : "true"));
        logOrderTraffic.set(Boolean.valueOf(
                overrides.containsKey(SETTING_LOG_ORDER_TRAFFIC) ? overrides.get(SETTING_LOG_ORDER_TRAFFIC) : "true"));

    }

    public Map<String,Boolean> getCustomLogConfiguration() {
        Map a = new HashMap<String,Boolean>();

        a.put(SETTING_LOG_MARKET_INCREMENTAL_REFRESH, logMarketIncrementalRefreshMessages.get());
        a.put(SETTING_LOG_QUOTE_TRAFIC, logQuoteTraffic.get());
        a.put(SETTING_LOG_ORDER_TRAFFIC, logOrderTraffic.get());

        return a;
    }

    @Override
    protected void logIncoming(String message) {
        char messageType = getMessageType(message);
        switch(messageType) {
            case 'D': // check for NewOrderSingle
                if(logOrderTraffic.get()) {
                    super.logIncoming(message);
                }
                break;
            case '8': // check for ExecutionReport
                if(logOrderTraffic.get()) {
                    super.logIncoming(message);
                }
                break;
            case 'X': // check for MarketDataIncrementalRefresh
                if(logMarketIncrementalRefreshMessages.get()) {
                    super.logIncoming(message);
                }
                break;
            case 'R': // check for Quote
                if(logQuoteTraffic.get()) {
                    super.logIncoming(message);
                }
                break;
            case 'S': // check for QuoteRequest
                if(logQuoteTraffic.get()) {
                    super.logIncoming(message);
                }
                break;
            default:
                super.logIncoming(message);
                break;
        }
    }

    @Override
    protected void logOutgoing(String message) {
        char messageType = getMessageType(message);
        switch(messageType) {
            case 'D': // check for NewOrderSingle
                if(logOrderTraffic.get()) {
                    super.logOutgoing(message);
                }
                break;
            case '8': // check for ExecutionReport
                if(logOrderTraffic.get()) {
                    super.logOutgoing(message);
                }
                break;
            case 'X': // check for MarketDataIncrementalRefresh
                if(logMarketIncrementalRefreshMessages.get()) {
                    super.logOutgoing(message);
                }
                break;
            case 'R': // check for Quote
                if(logQuoteTraffic.get()) {
                    super.logOutgoing(message);
                }
                break;
            case 'S': // check for QuoteRequest
                if(logQuoteTraffic.get()) {
                    super.logOutgoing(message);
                }
                break;
            default:
                super.logOutgoing(message);
                break;
        }
    }

    /*
        Identify the FIX message by its message type field 35=<x>
    */
    private char getMessageType(String message) {
        int idx = message.indexOf("35=")+3; // FIX standard requires ASCII so we don't have to worry about multibyte chars
        return idx >= 0 ? message.charAt(idx) : 0x00;
    }
}


