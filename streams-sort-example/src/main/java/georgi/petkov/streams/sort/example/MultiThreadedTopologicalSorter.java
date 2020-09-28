package georgi.petkov.streams.sort.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

class MultiThreadedTopologicalSorter {

    private final BiConsumer<UUID, Event> consumer;
    private final Map<UUID, List<Event>> waitingEventsByDependencyId;
    private final Map<UUID, UUID> topLevelDependencyIdByEventId;

    MultiThreadedTopologicalSorter(BiConsumer<UUID, Event> consumer) {
        this.consumer = consumer;
        this.waitingEventsByDependencyId = new ConcurrentHashMap<>();
        this.topLevelDependencyIdByEventId = new ConcurrentHashMap<>();
    }

    void consume(Event event) {
        if (!event.isDependent()) {
            consumeRecursively(event, event.getId());
            return;
        }

        /*
         * Following are the actions taken based on whether the dependency event is consumed and are there any waiting events.
         * Has Waiting Events  Consumed  Action (Add or Consume the current event)
         * false               false     Add // this is the first waiting event
         * false               true      Consume // the waiting events may have already been read and cleared
         * true                false     Add // this is yet another waiting event
         * true                true      Add/Consume // #compute() is blocking #remove() either way
         */
        List<Event> waitingEvents = waitingEventsByDependencyId.compute(event.getDependencyId(), (dependencyId, currentWaitingEvents) -> {
            if (currentWaitingEvents == null) {
                // it's the first event to wait or the waiting events are already read & cleared
                if (isEventConsumed(dependencyId)) {
                    // indicate no waiting is needed (it may be too late to add the event anyway)
                    return null;
                }

                currentWaitingEvents = new ArrayList<>();
            }

            currentWaitingEvents.add(event);

            return currentWaitingEvents;
        });

        if (waitingEvents == null) {
            // the current event was not added as an event waiting for its dependency
            consumeRecursively(event, topLevelDependencyIdByEventId.get(event.getDependencyId()));
        }
    }

    private boolean isEventConsumed(UUID id) {
        return topLevelDependencyIdByEventId.containsKey(id);
    }

    private void consumeRecursively(Event event, UUID topLevelDependencyId) {
        consumeEvent(event, topLevelDependencyId);
        Iterable<Event> dependants = waitingEventsByDependencyId.remove(event.getId());
        if (dependants != null) {
            dependants.forEach(t -> consumeRecursively(t, topLevelDependencyId));
        }
    }

    private void consumeEvent(Event event, UUID topLevelDependencyId) {
        topLevelDependencyIdByEventId.put(event.getId(), topLevelDependencyId);
        consumer.accept(topLevelDependencyId, event);
    }
}
