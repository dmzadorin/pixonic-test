package com.pixonic

import pixonic.test.EventScheduler
import pixonic.test.EventSchedulerImpl
import spock.lang.Shared
import spock.lang.Specification

import java.time.LocalDateTime
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong;

class EventSchedulerTest extends Specification {

    @Shared
    def scheduler

    def setupSpec() {
        scheduler = new EventSchedulerImpl<>()
    }

    def cleanupSpec() {
        scheduler.stopScheduler()
    }

    def "Test that event with empty datetime or callable is rejected"(){
        when:
        scheduler.scheduleEvent(null, {})
        then:
        thrown(IllegalArgumentException)
        when:
        scheduler.scheduleEvent(LocalDateTime.now(), null)
        then:
        thrown(IllegalArgumentException)
    }

    def "Test pushing events"() {
        setup:
        def latch = new CountDownLatch(3)
        def now = LocalDateTime.now()
        expect:
        scheduler.scheduleEvent(now.minusSeconds(5), sleepCallable(1, latch))
        scheduler.scheduleEvent(now.plusSeconds(10), sleepCallable(2, latch))
        Thread.sleep(100)
        scheduler.scheduleEvent(now.plusSeconds(10), sleepCallable(2, latch))
        latch.await()
    }

    def "Test that events are executed in correct order"() {
        setup:
        def latch = new CountDownLatch(3)
        def now = LocalDateTime.now()
        expect:
        def firstEvent = scheduleEvent(scheduler, now.plusSeconds(2), latch)
        def secondEvent = scheduleEvent(scheduler, now.plusSeconds(5), latch)
        def thirdEvent = scheduleEvent(scheduler, now, latch)
        latch.await()
        //Third event should be executed without waiting
        assert thirdEvent.get() < firstEvent.get()
        assert thirdEvent.get() < secondEvent.get()
        //Then first event should be executed, and finally second event
        assert firstEvent.get() < secondEvent.get()
    }

    def "Test that events with same start time are executed in order of appearance"() {
        setup:
        def latch = new CountDownLatch(3)
        def now = LocalDateTime.now()
        expect:
        def firstEvent = scheduleEvent(scheduler, now.plusSeconds(3), latch)
        Thread.sleep(100)
        def secondEvent = scheduleEvent(scheduler, now.plusSeconds(3), latch)
        Thread.sleep(100)
        def thirdEvent = scheduleEvent(scheduler, now.plusSeconds(3), latch)
        latch.await()
        //First event time should be greater than second time
        assert firstEvent.get() < secondEvent.get()
        assert secondEvent.get() < thirdEvent.get()
    }

    def scheduleEvent(EventScheduler scheduler, def startTime, CountDownLatch latch) {
        def timeRef = new AtomicLong()
        def callable = {
            def start = System.currentTimeMillis()
            Thread.sleep(1000)
            timeRef.set(start)
            latch.countDown()
            return null
        }
        scheduler.scheduleEvent(startTime, callable as Callable)
        return timeRef
    }

    def sleepCallable(def seconds, CountDownLatch latch) {
        return new Callable<Object>() {

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