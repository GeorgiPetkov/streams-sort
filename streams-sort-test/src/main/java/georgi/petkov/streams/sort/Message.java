package georgi.petkov.streams.sort;

import lombok.Value;

import java.util.UUID;

import static java.util.Objects.requireNonNull;

@Value
class Message {

    UUID id;
    UUID partOfId;
    MessageType type;
    int number;
    String content;

    Message(UUID id, UUID partOfId, MessageType type, int number, String content) {
        this.id = requireNonNull(id);
        this.partOfId = partOfId;
        this.type = requireNonNull(type);
        this.number = number;
        this.content = requireNonNull(content);
    }
}
