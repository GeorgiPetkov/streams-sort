package georgi.petkov.streams.sort;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serde;

import java.time.Duration;

import static java.lang.Long.parseLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.common.serialization.Serdes.serdeFrom;

class Serdes {

    private static final Gson GSON = new Gson();

    static final Serde<Message> MESSAGE_SERDE = serdeFrom(
            (topic, message) -> messageToJson(message).getBytes(UTF_8),
            (topic, bytes) -> GSON.fromJson(bytesToString(bytes), Message.class));

    static String messageToJson(Message message) {
        return GSON.toJson(message);
    }

    static final Serde<Duration> DURATION_SERDE = serdeFrom(
            (topic, duration) -> Long.toString(duration.toMillis()).getBytes(UTF_8),
            (topic, bytes) -> stringToDuration(bytesToString(bytes)));

    static Duration stringToDuration(String duration) {
        return Duration.ofMillis(parseLong(duration));
    }

    private static String bytesToString(byte[] bytes) {
        return new String(bytes, UTF_8);
    }

    private Serdes() {
    }
}
