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
        def scheduler = new EventSchedulerImpl()
        def latch = new CountDownLatch(3)
        def now = LocalDateTime.now()
        expect:
        scheduler.scheduleEvent(now.minusSeconds(5), sleepCallable(1, latch))
        scheduler.scheduleEvent(now.plusSeconds(10), sleepCallable(2, latch))
        Thread.sleep(100)
        scheduler.scheduleEvent(now.plusSeconds(10), sleepCallable(2, latch))
        latch.await()
    }


    def sleepCallable(def seconds, CountDownLatch latch) {
        return new Callable<Object>(){

            @Override
            Object call() throws Exception {
                def millis = TimeUnit.SECONDS.toMillis(seconds)
                println("Sleeping for $seconds seconds")
                Thread.sleep(millis)
                println("Finished sleep")
                latch.countDown()
                return null
            }
        }
    }
}