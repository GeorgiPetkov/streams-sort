package georgi.petkov.streams.sort;

import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static georgi.petkov.streams.sort.EventSerde.EVENT_SERDE;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static org.apache.kafka.common.serialization.Serdes.UUID;
import static org.apache.kafka.streams.KeyValue.pair;

class LogicallyPartitionedStreamTopologicalSorterTest extends StreamTopologicalSorterTest {

    @Test
    void assertCorrectOrder() {
        UUID correlationId = randomUUID();
        UUID dependencyId = randomUUID();
        UUID dependentId1 = randomUUID();
        UUID dependentId11 = randomUUID();
        UUID dependentId12 = randomUUID();
        UUID dependentId13 = randomUUID();
        UUID dependentId2 = randomUUID();
        UUID dependentId3 = randomUUID();

        test(
                asList(
                        pair(correlationId, new Event(dependentId12, dependentId1, "dependent12")),
                        pair(correlationId, new Event(dependentId12, dependentId1, "dependent12")),
                        pair(correlationId, new Event(dependentId11, dependentId1, "dependent11")),
                        pair(correlationId, new Event(dependentId11, dependentId1, "dependent11")),
                        pair(correlationId, new Event(dependentId1, dependencyId, "dependent1")),
                        pair(correlationId, new Event(dependentId1, dependencyId, "dependent1")),
                        pair(correlationId, new Event(dependentId2, dependencyId, "dependent2")),
                        pair(correlationId, new Event(dependentId2, dependencyId, "dependent2")),
                        pair(correlationId, new Event(dependencyId, null, "dependency")),
                        pair(correlationId, new Event(dependencyId, null, "dependency")),
                        pair(correlationId, new Event(dependentId3, dependencyId, "dependent3")),
                        pair(correlationId, new Event(dependentId3, dependencyId, "dependent3")),
                        pair(correlationId, new Event(dependentId13, dependentId1, "dependent13")),
                        pair(correlationId, new Event(dependentId13, dependentId1, "dependent13"))),
                asList(
                        pair(correlationId, "dependency"),
                        pair(correlationId, "dependent1"),
                        pair(correlationId, "dependent2"),
                        pair(correlationId, "dependent12"),
                        pair(correlationId, "dependent11"),
                        pair(correlationId, "dependent3"),
                        pair(correlationId, "dependent13")));
    }

    @Override
    Sorter getSorter() {
        return (input, builder) -> LogicallyPartitionedStreamTopologicalSorter.sort(
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
