package georgi.petkov.streams.sort.example;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.out;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MultiThreadedTopologicalSorterTest extends TopologicalSorterTest {

    private static final int PARALLELISM = 4;

    private final Set<Event> consumedEvents = ConcurrentHashMap.newKeySet();
    private final Map<UUID, Set<Event>> consumedEventsByTopLevelDependencyId = new ConcurrentHashMap<>();
    private final MultiThreadedTopologicalSorter sorter = new MultiThreadedTopologicalSorter((topLevelDependencyId, event) -> {
        consumedEvents.add(event);
        consumedEventsByTopLevelDependencyId.computeIfAbsent(topLevelDependencyId, id -> ConcurrentHashMap.newKeySet()).add(event);
    });

    @SneakyThrows
    @Test
    void generalTest() {
        long startTime = currentTimeMillis();

        ForkJoinPool forkJoinPool = new ForkJoinPool(PARALLELISM);
        try {
            forkJoinPool.submit(() -> ALL_EVENTS.parallelStream().forEach(sorter::consume)).get();
        } finally {
            forkJoinPool.shutdown();
        }

        long endTime = currentTimeMillis();
        long time = endTime - startTime;
        out.printf("Time for %s to process %d events with parallelism %d: %dms (%d events per second)%n",
                sorter.getClass().getSimpleName(), ALL_EVENTS.size(), PARALLELISM, time, ALL_EVENTS.size() * 1000L / time);

        assertEquals(EXPECTED_CONSUMED_EVENTS, consumedEvents);
        assertEquals(EXPECTED_CONSUMED_EVENTS_BY_TOP_LEVEL_DEPENDENCY_ID, consumedEventsByTopLevelDependencyId);
    }

    @Test
    void generalTestWithoutParallelExecution() {
        long startTime = currentTimeMillis();

        ALL_EVENTS.forEach(sorter::consume);

        long endTime = currentTimeMillis();
        long time = endTime - startTime;
        out.printf("Time for %s to process %d events sequentially: %dms (%d events per second)%n",
                sorter.getClass().getSimpleName(), ALL_EVENTS.size(), time, ALL_EVENTS.size() * 1000L / time);

        assertEquals(EXPECTED_CONSUMED_EVENTS, consumedEvents);
        assertEquals(EXPECTED_CONSUMED_EVENTS_BY_TOP_LEVEL_DEPENDENCY_ID, consumedEventsByTopLevelDependencyId);
    }
}
