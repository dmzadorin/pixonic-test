package com.pixonic

import spock.lang.Shared
import spock.lang.Specification

import java.time.LocalDateTime
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class EventSchedulerTest extends Specification {

    @Shared
    def scheduler

    def setupSpec() {
        scheduler = new EventSchedulerImpl<>()
    }

    def cleanupSpec() {
        scheduler.stopScheduler()
    }

    def "Test that event with empty datetime or callable is rejected"() {
        when:
        scheduler.scheduleEvent(null, {})
        then:
        thrown(IllegalArgumentException)
        when:
        scheduler.scheduleEvent(LocalDateTime.now(), null)
        then:
        thrown(IllegalArgumentException)
    }

    def "Test pushing events with different start times"() {
        setup:
        def latch = new CountDownLatch(4)
        def now = LocalDateTime.now()
        expect:
        def elapsedOne = scheduleEvent(scheduler, now.minusSeconds(5), latch)
        def elapsedTwo = scheduleEvent(scheduler, now.minusSeconds(3), latch)
        def futureOne = scheduleEvent(scheduler, now.plusSeconds(10), latch)
        Thread.sleep(100)
        def futureTwo = scheduleEvent(scheduler, now.plusSeconds(10), latch)
        latch.await()
        assert elapsedOne.get() < elapsedTwo.get()
        assert elapsedTwo.get() < futureOne.get()
//        Second scheduled event is executed after first scheduled event
        assert futureOne.get() < futureTwo.get()
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
        def callable = new Callable<Long>() {
            @Override
            Long call() throws Exception {
                def start = System.currentTimeMillis()
                println("Callable start time: $start")
                Thread.sleep(1000)
                println("Finished sleep for 1 second")
                latch.countDown()
                return start
            }
        }
        return scheduler.scheduleEvent(startTime, callable)
    }

}