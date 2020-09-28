package georgi.petkov.streams.sort.example;

import java.util.*;

import static georgi.petkov.streams.sort.example.Event.createDependentEvent;
import static georgi.petkov.streams.sort.example.Event.createIndependentEvent;
import static java.util.Collections.shuffle;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toSet;

abstract class TopologicalSorterTest {

    private static final int TREES_COUNT = 60;
    private static final int TREE_GROWTH_FACTOR = 3;
    private static final int TREE_DEPTH = 8;

    protected static final Map<UUID, Set<Event>> EXPECTED_CONSUMED_EVENTS_BY_TOP_LEVEL_DEPENDENCY_ID;
    protected static final Set<Event> EXPECTED_CONSUMED_EVENTS;
    protected static final List<Event> ALL_EVENTS;

    static {
        EXPECTED_CONSUMED_EVENTS_BY_TOP_LEVEL_DEPENDENCY_ID = new HashMap<>();
        for (int i = 0; i < TREES_COUNT; i++) {
            UUID topLevelDependencyId = randomUUID();
            EXPECTED_CONSUMED_EVENTS_BY_TOP_LEVEL_DEPENDENCY_ID.put(topLevelDependencyId, createTree(topLevelDependencyId));
        }

        EXPECTED_CONSUMED_EVENTS = EXPECTED_CONSUMED_EVENTS_BY_TOP_LEVEL_DEPENDENCY_ID.values().stream()
                .flatMap(Collection::stream)
                .collect(toSet());

        ALL_EVENTS = new ArrayList<>(EXPECTED_CONSUMED_EVENTS);
        shuffle(ALL_EVENTS);
    }

    private static Set<Event> createTree(UUID topLevelDependencyId) {
        Set<Event> result = new HashSet<>();
        Event event = createIndependentEvent(topLevelDependencyId);
        result.add(event);
        createTree(event, TREE_DEPTH, result);

        return result;
    }

    private static void createTree(Event dependency, int depth, Set<Event> result) {
        if (depth == 0) {
            return;
        }

        for (int i = 0; i < TREE_GROWTH_FACTOR; i++) {
            Event event = createDependentEvent(randomUUID(), dependency);
            result.add(event);
            createTree(event, depth - 1, result);
        }
    }
}
