package georgi.petkov.streams.sort.example;

import java.util.*;
import java.util.function.BiConsumer;

class SingleThreadedTopologicalSorter {

    private final BiConsumer<UUID, Event> consumer;
    private final Map<UUID, List<Event>> waitingEventsByDependencyId;
    private final Map<UUID, UUID> topLevelDependencyIdByEventId;

    SingleThreadedTopologicalSorter(BiConsumer<UUID, Event> consumer) {
        this.consumer = consumer;
        this.waitingEventsByDependencyId = new HashMap<>();
        this.topLevelDependencyIdByEventId = new HashMap<>();
    }

    void consume(Event event) {
        if (!event.isDependent()) {
            consumeRecursively(event, event.getId());
            return;
        }

        if (isEventConsumed(event.getDependencyId())) {
            consumeRecursively(event, topLevelDependencyIdByEventId.get(event.getDependencyId()));
            return;
        }

        waitingEventsByDependencyId.computeIfAbsent(event.getDependencyId(), unused -> new ArrayList<>()).add(event);
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
