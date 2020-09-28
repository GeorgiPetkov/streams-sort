package georgi.petkov.streams.sort;

import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static georgi.petkov.streams.sort.EventSerde.EVENT_SERDE;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static org.apache.kafka.common.serialization.Serdes.UUID;
import static org.apache.kafka.streams.KeyValue.pair;

class RelationallyPartitionedStreamTopologicalSorterTest extends StreamTopologicalSorterTest {

    @Test
    void assertCorrectOrder() {
        UUID dependencyId = randomUUID();
        UUID dependentId1 = randomUUID();
        UUID dependentId11 = randomUUID();
        UUID dependentId12 = randomUUID();
        UUID dependentId13 = randomUUID();
        UUID dependentId2 = randomUUID();
        UUID dependentId3 = randomUUID();

        test(
                asList(
                        pair(dependentId1, new Event(dependentId12, dependentId1, "dependent12")),
                        pair(dependentId1, new Event(dependentId12, dependentId1, "dependent12")),
                        pair(dependentId1, new Event(dependentId11, dependentId1, "dependent11")),
                        pair(dependentId1, new Event(dependentId11, dependentId1, "dependent11")),
                        pair(dependencyId, new Event(dependentId1, dependencyId, "dependent1")),
                        pair(dependencyId, new Event(dependentId1, dependencyId, "dependent1")),
                        pair(dependencyId, new Event(dependentId2, dependencyId, "dependent2")),
                        pair(dependencyId, new Event(dependentId2, dependencyId, "dependent2")),
                        pair(dependencyId, new Event(dependencyId, null, "dependency")),
                        pair(dependencyId, new Event(dependencyId, null, "dependency")),
                        pair(dependencyId, new Event(dependentId3, dependencyId, "dependent3")),
                        pair(dependencyId, new Event(dependentId3, dependencyId, "dependent3")),
                        pair(dependentId1, new Event(dependentId13, dependentId1, "dependent13")),
                        pair(dependentId1, new Event(dependentId13, dependentId1, "dependent13"))),
                asList(
                        pair(dependencyId, "dependency"),
                        pair(dependencyId, "dependent1"),
                        pair(dependencyId, "dependent2"),
                        pair(dependencyId, "dependent12"),
                        pair(dependencyId, "dependent11"),
                        pair(dependencyId, "dependent3"),
                        pair(dependencyId, "dependent13")));
    }

    @Override
    Sorter getSorter() {
        return (input, builder) -> RelationallyPartitionedStreamTopologicalSorter.sort(
                input,
                builder,
                UUID(),
                EVENT_SERDE,
                TOPOLOGY_PREFIX,
                EVENT_ID_EXTRACTOR,
                DEPENDENCY_EVENT_ID_EXTRACTOR,
                WINDOWS,
                Stores::persistentWindowStore);
    }
}