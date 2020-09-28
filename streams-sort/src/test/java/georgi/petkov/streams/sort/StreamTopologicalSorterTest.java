package georgi.petkov.streams.sort;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;

import static georgi.petkov.streams.sort.EventSerde.EVENT_SERDE;
import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.common.serialization.Serdes.UUID;
import static org.apache.kafka.streams.StreamsConfig.*;
import static org.apache.kafka.streams.kstream.Consumed.with;
import static org.apache.kafka.streams.kstream.Produced.valueSerde;
import static org.apache.kafka.streams.kstream.internals.KStreamHacks.requiresRepartition;
import static org.junit.jupiter.api.Assertions.*;

abstract class StreamTopologicalSorterTest {

    @FunctionalInterface
    interface Sorter {

        KStream<UUID, Event> sort(KStream<UUID, Event> input, StreamsBuilder builder);
    }

    static final String TOPOLOGY_PREFIX = "test-";
    static final Function<Event, UUID> EVENT_ID_EXTRACTOR = Event::getId;
    static final Function<Event, UUID> DEPENDENCY_EVENT_ID_EXTRACTOR = Event::getDependencyId;
    static final JoinWindows WINDOWS = JoinWindows.of(Duration.ofDays(1)).grace(Duration.ofDays(1));

    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";

    private static final Properties BUILDER_PROPERTIES = new Properties();

    static {
        BUILDER_PROPERTIES.put(TOPOLOGY_OPTIMIZATION, OPTIMIZE);
    }

    private static final Properties STREAMS_PROPERTIES = new Properties();

    static {
        STREAMS_PROPERTIES.put(APPLICATION_ID_CONFIG, "test-application-id");
        STREAMS_PROPERTIES.put(BOOTSTRAP_SERVERS_CONFIG, "test-bootstrap-servers");
    }

    abstract Sorter getSorter();

    @Test
    void assertRequiringAlreadyRepartitionedStream() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<UUID, Event> input = builder.stream(INPUT_TOPIC, with(UUID(), EVENT_SERDE))
                .map(KeyValue::pair); // mark as requiring repartition

        assertThrows(IllegalArgumentException.class, () -> getSorter().sort(input, builder));
    }

    final void test(
            List<KeyValue<UUID, Event>> input,
            List<KeyValue<UUID, String>> expectedOutput) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<UUID, Event> sortedStream = getSorter().sort(builder.stream(INPUT_TOPIC, with(UUID(), EVENT_SERDE)), builder);

        assertFalse(requiresRepartition(sortedStream));

        sortedStream
                .mapValues(Event::getData)
                .to(OUTPUT_TOPIC, valueSerde(String()));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(BUILDER_PROPERTIES), STREAMS_PROPERTIES)) {
            TestInputTopic<UUID, Event> inputTopic = driver.createInputTopic(INPUT_TOPIC, UUID().serializer(), EVENT_SERDE.serializer());
            TestOutputTopic<UUID, String> outputTopic = driver.createOutputTopic(OUTPUT_TOPIC, UUID().deserializer(), new StringDeserializer());

            inputTopic.pipeKeyValueList(input);
            List<KeyValue<UUID, String>> expected = outputTopic.readKeyValuesToList();
            assertIterableEquals(expected, expectedOutput);
        }
    }
}
