package eu.openaire.publications_retriever.util.http;

import java.time.Instant;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DomainConnectionData {

    Instant lastTimeConnected;

    int timesConnected; // This is just for statistics, at the moment.

    final Lock lock = new ReentrantLock(true);  // This lock is locking the threads on each domain.

    public DomainConnectionData() {
        this.timesConnected = 1;
        this.lastTimeConnected = Instant.now();
    }


    /**
     * This method assumes that the lock is already locked.
     * */
    public void updateAndUnlock(Instant currentTime) {
        this.lastTimeConnected = currentTime;
        this.timesConnected ++;
        this.lock.unlock();
    }

    public int getTimesConnected() {
        return this.timesConnected;
    }

}
