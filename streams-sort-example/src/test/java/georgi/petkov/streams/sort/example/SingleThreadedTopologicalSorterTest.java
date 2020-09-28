package georgi.petkov.streams.sort.example;

import org.junit.jupiter.api.Test;

import java.util.*;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.out;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SingleThreadedTopologicalSorterTest extends TopologicalSorterTest {

    private final Set<Event> consumedEvents = new HashSet<>();
    private final Map<UUID, Set<Event>> consumedEventsByTopLevelDependencyId = new HashMap<>();
    private final SingleThreadedTopologicalSorter sorter = new SingleThreadedTopologicalSorter((topLevelDependencyId, event) -> {
        consumedEvents.add(event);
        consumedEventsByTopLevelDependencyId.computeIfAbsent(topLevelDependencyId, id -> new HashSet<>()).add(event);
    });

    @Test
    void generalTest() {
        long startTime = currentTimeMillis();

        ALL_EVENTS.forEach(sorter::consume);

        long endTime = currentTimeMillis();
        long time = endTime - startTime;
        out.printf("Time for %s to process %d events: %dms (%d events per second)%n",
                sorter.getClass().getSimpleName(), ALL_EVENTS.size(), time, ALL_EVENTS.size() * 1000L / time);

        assertEquals(EXPECTED_CONSUMED_EVENTS, consumedEvents);
        assertEquals(EXPECTED_CONSUMED_EVENTS_BY_TOP_LEVEL_DEPENDENCY_ID, consumedEventsByTopLevelDependencyId);
    }
}
