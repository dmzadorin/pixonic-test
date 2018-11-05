package com.pixonic

import pixonic.test.EventSchedulerImpl
import spock.lang.Specification

import java.time.LocalDateTime
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit;

class EventSchedulerTest extends Specification {

    def "Test pushing event"(){
        setup:
        def scheduler = new EventSchedulerImpl(2)
        def latch = new CountDownLatch(5)
        def now = LocalDateTime.now()
        expect:
        scheduler.scheduleEvent(now.minusMinutes(5), sleepCallable(2, latch))
        scheduler.scheduleEvent(now.minusMinutes(3), sleepCallable(2, latch))
        scheduler.scheduleEvent(now, sleepCallable(2, latch))
        scheduler.scheduleEvent(now.minusMinutes(2), sleepCallable(1, latch))
        scheduler.scheduleEvent(now, sleepCallable(1, latch))
        latch.await()
    }


    def sleepCallable(def seconds, CountDownLatch latch) {
        return new Callable<Object>(){

            @Override
            Object call() throws Exception {
                def millis = TimeUnit.SECONDS.toMillis(seconds)
                Thread.sleep(millis)
                latch.countDown()
                return null
            }
        }
    }
}