package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import org.apache.commons.math3.util.Pair;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by thejus on 13/9/17.
 */
public class SearchSessions implements Serializable{

    @JsonProperty(value = "sessions")
    Map<String, SearchSession> sessions;

    int lastPosition;

    String lastFindingMethod;

    String lastSQID;

    @JsonCreator
    public SearchSessions(@JsonProperty(value = "sessions") Map<String, SearchSession> sessions) {
        this.sessions = sessions;
        this.lastFindingMethod = null;
        this.lastSQID = null;
    }

    public SearchSessions() {
        sessions = new LinkedHashMap<>();
        lastSQID = UUID.randomUUID().toString();
    }

    public Map<String, SearchSession> getSessions() {
        return sessions;
    }

    @JsonIgnore
    private void setSessions(Map<String,SearchSession> sessions) {
        this.sessions = sessions;
    }

    public void add(String sqid, String searchQuery, ProductObj product) {
        String currentFindingMethod = product.getFindingmethod();
        int currentPos = product.getPosition();
        if (sqid == null) {
            if (lastFindingMethod == null || !lastFindingMethod.equals(currentFindingMethod) || currentPos <= lastPosition) {
                sqid = UUID.randomUUID().toString();
                lastSQID = sqid;
            } else {
                sqid = lastSQID;
            }
        }
        if (!sessions.containsKey(sqid)) {
            sessions.put(sqid, new SearchSession(sqid, searchQuery, product.getTimestamp()));
        }
        sessions.get(sqid).add(product);
        lastFindingMethod = currentFindingMethod;
        lastPosition = currentPos;
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

        return new Pair<>(allDaysStats, stats);
    }

    public static SearchSessions mergeSessions(List<SearchSessions> sessionsList) {
        if (sessionsList.size() == 1) {
            return sessionsList.get(0);
        } else {
            SearchSessions finalSessions = new SearchSessions();
            ArrayList<SearchSession> mergedSessions = new ArrayList<>();
            for (SearchSessions searchSessions : sessionsList) {
                mergedSessions.addAll(searchSessions.getSessions().values());

            }
            mergedSessions.sort(Comparator.comparingLong(SearchSession::getTimestamp));
            Map<String, SearchSession> mergedSessionsMap = mergedSessions.stream().collect(Collectors.toMap(SearchSession::getSqid, Function.identity(),
                    (u, v) -> v,
                    LinkedHashMap::new));
            finalSessions.setSessions(mergedSessionsMap);
            return finalSessions;
        }
    }

    public static SearchSessions filterSessions(SearchSessions sessions, Predicate<ProductObj> predicate) {

        SearchSessions filteredSessions = new SearchSessions();
        Map<String, SearchSession> sessionsMap = new HashMap<String, SearchSession>();

        for (SearchSession searchSession : sessions.getSessions().values()) {
            searchSession = searchSession.clone();
            searchSession.filterProducts(predicate);
            if(searchSession.numImpressions() > 0) {
                sessionsMap.put(searchSession.getSqid(), searchSession);
            }
        }

        filteredSessions.setSessions(sessionsMap);
        return filteredSessions;
    }
}
