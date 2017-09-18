package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import org.apache.commons.math3.util.Pair;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by thejus on 13/9/17.
 */
class SearchSessions implements Serializable{

    Map<String, SearchSession> sessions;
    String lastFindingMethod;
    String lastSQID;

    public SearchSessions() {
        sessions = new LinkedHashMap<>();
        lastSQID = UUID.randomUUID().toString();
    }

    public Map<String, SearchSession> getSessions() {
        return sessions;
    }

    public void add(String sqid, ProductObj product) {
        String currentFindingMethod = product.getFindingmethod();
        if (sqid == null) {
            if (lastFindingMethod == null || !lastFindingMethod.equals(currentFindingMethod)) {
                sqid = UUID.randomUUID().toString();
                lastSQID = sqid;
            } else {
                sqid = lastSQID;
            }
        }
        if (!sessions.containsKey(sqid)) {
            sessions.put(sqid, new SearchSession(sqid, product.getTimestamp()));
        }
        sessions.get(sqid).add(product);
        lastFindingMethod = currentFindingMethod;
    }

    @Override
    public String toString() {
        return "SearchSessions{" +
                "sessions=" + sessions +
                '}';
    }

    private static SimpleDateFormat format = new SimpleDateFormat("dd/MM/YY");


    @JsonIgnore
    public Pair<SessionsStats, Map<String, SessionsStats>> getStats() {
        Map<String, SessionsStats> stats = new LinkedHashMap<>();
        SessionsStats allDaysStats = new SessionsStats();

        sessions.values().stream().forEach(
                session -> {
                    String date = format.format(session.getTimestamp());
                    if(!stats.containsKey(date)) {
                        stats.put(date, new SessionsStats());
                    }
                    SessionsStats sessionsStats = stats.get(date);

                    sessionsStats.incNumSessions();
                    sessionsStats.incNumImpressions(session.numImpressions());
                    sessionsStats.incNumClicks(session.numClicks());
                    sessionsStats.incNumBuys(session.numBuys());

                    allDaysStats.incNumSessions();
                    allDaysStats.incNumImpressions(session.numImpressions());
                    allDaysStats.incNumClicks(session.numClicks());
                    allDaysStats.incNumBuys(session.numBuys());
                }
        );

        allDaysStats.setNumDays(stats.keySet().size());

        return new Pair<SessionsStats, Map<String, SessionsStats>>(allDaysStats, stats);
    }

}
