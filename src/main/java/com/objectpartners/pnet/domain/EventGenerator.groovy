package com.objectpartners.pnet.domain

import com.peoplenet.eventledger.domain.Event
import com.peoplenet.eventledger.domain.EventId
import org.joda.time.DateTime

class EventGenerator {

    final static int ttl = 60 * 60 * 24

    static List<Event> createEventList() {

        final List<Event> eventList = []
        final def emitterIds = ['EMITTER-A', 'EMITTER-B', 'EMITTER-C']
        final def eventType = ['VIN-DISCOVERY', 'HARD-BRAKE', 'ENGINE-ON', 'ENGINE-OFF', 'TEMP-WARN']
        final def eventIds = 1..4

        def year = 2015
        def monthOfYear = 10
        def day = 1..8
        def hour = 1..23
        def min = 1..59
        def sec = [10, 20, 30, 40, 50]

        day.each {
            def dayOfMonth = it
            hour.each {
                def hourOfDay = it
                min.each {
                    def minuteOfHour = it
                    sec.each {
                        def secondOfMinute = it
                        int idx = it / 10

                        DateTime dateTime = new DateTime(
                                year,
                                monthOfYear,
                                dayOfMonth,
                                hourOfDay,
                                minuteOfHour,
                                secondOfMinute)

                        emitterIds.each {
                            def id = it
                            eventType.each {
                                eventIds.each {
                                    def eventId = new EventId(
                                            emitterId: id,
                                            emitterType: 'TEST',
                                            time: dateTime,
                                            eventId: it)

                                    def type = eventType[idx]
                                    eventList << new Event(
                                            eventKey: eventId,
                                            eventType: type,
                                            ttl: ttl,
                                            writeTime: (new DateTime()),
                                            body: "I AM DATA"
                                    )
                                }
                            }
                        }
                    }
                }
            }
        }
        eventList
    }

    public static void main(String[] args) {
        def events = createEventList();
        println events.size()
    }

}

