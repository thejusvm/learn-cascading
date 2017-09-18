package com.flipkart.learn.cascading.cdm_data_selection;

/**
 * Created by thejus on 14/9/17.
 */
public class SessionsStats {

    int numDays = 1;
    int numSessions = 0;
    int numSessionClicks = 0;
    int numClicks = 0;
    int numSessionBuys = 0;
    int numBuys = 0;
    int numImpressions = 0;

    public int getNumDays() {
        return numDays;
    }

    public int getNumSessions() {
        return numSessions;
    }

    public int getNumSessionClicks() {
        return numSessionClicks;
    }

    public int getNumClicks() {
        return numClicks;
    }

    public int getNumSessionBuys() {
        return numSessionBuys;
    }

    public int getNumBuys() {
        return numBuys;
    }

    public int getNumImpressions() {
        return numImpressions;
    }

    public void setNumDays(int numDays) {
        this.numDays = numDays;
    }

    public void incNumSessions() {
        numSessions++;
    }

    public void incNumClicks(int numClicks) {
        this.numClicks += numClicks;
        numSessionClicks += numClicks == 0 ? 0 : 1;
    }

    public void incNumBuys(int numBuys) {
        this.numBuys += numBuys;
        numSessionBuys += numBuys == 0 ? 0 : 1;
    }

    public void incNumImpressions(int numImps) {
        this.numImpressions += numImps;
    }
}
