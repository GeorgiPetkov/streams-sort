package georgi.petkov.streams.sort;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.common.serialization.Serdes.serdeFrom;

class EventSerde {

    private static final int UUID_SIZE = 16;

    private static final Serializer<Event> EVENT_SERIALIZER = (topic, event) -> {
        byte[] dataBytes = event.getData().getBytes(UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(UUID_SIZE + UUID_SIZE + dataBytes.length);

        putUUID(buffer, event.getId());
        putUUID(buffer, event.getDependencyId());
        buffer.put(dataBytes);

        return buffer.array();
    };

    private static final Deserializer<Event> EVENT_DESERIALIZER = (topic, bytes) -> {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        UUID id = getUUID(buffer);
        UUID dependencyId = getUUID(buffer);
        String data = UTF_8.decode(buffer).toString();

        return new Event(id, dependencyId, data);
    };

    static final Serde<Event> EVENT_SERDE = serdeFrom(EVENT_SERIALIZER, EVENT_DESERIALIZER);

    private static final UUID NULL_UUID = new UUID(0L, 0L);

    private static void putUUID(ByteBuffer to, UUID id) {
        UUID nonNullId = (id == null) ? NULL_UUID : id;
        to.putLong(nonNullId.getMostSignificantBits()).putLong(nonNullId.getLeastSignificantBits());
    }

    private static UUID getUUID(ByteBuffer from) {
        UUID id = new UUID(from.getLong(), from.getLong());
        return id.equals(NULL_UUID) ? null : id;
    }

    private EventSerde() {
    }
}
