package georgi.petkov.streams.sort;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static georgi.petkov.streams.sort.Constants.*;
import static georgi.petkov.streams.sort.InputOrder.*;
import static georgi.petkov.streams.sort.MessageType.*;
import static georgi.petkov.streams.sort.RelationallyPartitionedStreamTopologicalSorter.keySelection;
import static georgi.petkov.streams.sort.Serdes.messageToJson;
import static georgi.petkov.streams.sort.SortAlgorithm.LOGICAL;
import static georgi.petkov.streams.sort.SortAlgorithm.RELATIONAL;
import static java.lang.Integer.parseInt;
import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static java.lang.ProcessBuilder.Redirect.PIPE;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;

@Slf4j
class StreamsSortTest {

    private static final File WORK_DIR = new File(getenv(WORK_DIR_VARIABLE));
    private static final File TIMES_OUTPUT = new File(WORK_DIR, "times.csv");
    private static final int KAFKA_PORT = 9092;
    private static final String BOOTSTRAP_SERVER = format("localhost:%d", KAFKA_PORT);
    private static final String KEY_SEPARATOR = ":";

    static InputOrder inputOrder;
    static boolean duplicateInput;
    static SortAlgorithm sortAlgorithm;
    static int partitions;
    static int instances;

    public static void main(String[] args) throws IOException, InterruptedException {
        Files.deleteIfExists(TIMES_OUTPUT.toPath());
        Files.copy(
                StreamsSortTest.class.getResourceAsStream("/docker-compose.yml"),
                new File(WORK_DIR, "docker-compose.yml").toPath(),
                REPLACE_EXISTING);

        for (InputOrder inputOrder : asList(SHUFFLED,REVERSED)) {
            StreamsSortTest.inputOrder = inputOrder;
            for (boolean duplicateInput : asList(true)) {
                StreamsSortTest.duplicateInput = duplicateInput;
                for (SortAlgorithm sortAlgorithm : asList(RELATIONAL)) {
                    StreamsSortTest.sortAlgorithm = sortAlgorithm;
                    File input = prepareInput();
                    partitions = 1;
                    instances = 1;
                    runTest(input);
                    partitions = 4;
                    instances = 1;
                    runTest(input);
                    partitions = 4;
                    instances = 2;
                    runTest(input);
                    partitions = 4;
                    instances = 4;
                    runTest(input);
                    partitions = 16;
                    instances = 4;
                    runTest(input);
                }
            }
        }
    }

    private static File prepareInput() throws IOException, InterruptedException {
        File input = new File(WORK_DIR, "input.txt");
        File temp = new File(WORK_DIR, "temp");
        generateInput(input);

        if (duplicateInput) {
            duplicateInput(input, temp);
        }

        reorderInput(input, temp);

        return input;
    }

    private static void generateInput(File input) throws IOException {
        int booksCount = parseInt(getenv(BOOKS_VARIABLE));
        int chaptersPerBook = parseInt(getenv(CHAPTERS_VARIABLE));
        int paragraphsPerChapter = parseInt(getenv(PARAGRAPHS_VARIABLE));
        int wordsPerParagraph = parseInt(getenv(WORDS_VARIABLE));
        try (PrintWriter writer = new PrintWriter(input, UTF_8)) {
            range(0, booksCount).forEach(bookNumber -> {
                UUID bookId = randomUUID();
                String bookTitle = "Book: " + bookNumber;
                printMessage(writer, bookId, new Message(bookId, null, BOOK, bookNumber, bookTitle));
                range(0, chaptersPerBook).forEach(chapterNumber -> {
                    UUID chapterId = randomUUID();
                    String chapterTitle = "Chapter: " + chapterNumber;
                    printMessage(writer, bookId, new Message(chapterId, bookId, CHAPTER, chapterNumber, chapterTitle));
                    range(0, paragraphsPerChapter).forEach(paragraphNumber -> {
                        UUID paragraphId = randomUUID();
                        printMessage(writer, bookId, new Message(paragraphId, chapterId, PARAGRAPH, paragraphNumber, ""));
                        range(0, wordsPerParagraph).forEach(wordNumber ->
                                printMessage(writer, bookId, new Message(randomUUID(), paragraphId, WORD, wordNumber, "word" + wordNumber)));
                    });
                });
            });
        }
    }

    private static void printMessage(PrintWriter writer, UUID bookId, Message message) {
        UUID key = switch (sortAlgorithm) {
            case LOGICAL -> bookId;
            case RELATIONAL -> keySelection(Message::getId, Message::getPartOfId).apply(null, message);
        };
        writer.printf("%s%s%s%n", key, KEY_SEPARATOR, messageToJson(message));
    }

    private static void duplicateInput(File input, File temp) throws IOException, InterruptedException {
        executeCommand(format("cp %s %s; cat %s >> %s; mv %s %s", input, temp, input, temp, temp, input));
    }

    private static void reorderInput(File input, File temp) throws IOException, InterruptedException {
        switch (inputOrder) {
            case ORDERED -> {
            }
            case SHUFFLED -> executeCommand(format("shuf %s > %s; mv %s %s", input, temp, temp, input));
            case REVERSED -> executeCommand(format("tac %s > %s; mv %s %s", input, temp, temp, input));
            default -> throw new IllegalStateException(inputOrder.name());
        }
    }

    private static void runTest(File input) throws IOException, InterruptedException {
        removeAllContainers();

        log.info("Test started.");
        try {
            startKafkaAsync();
            createTopics();
            populateEventsInKafka(input);

            startApp();

            reportTimes();
        } finally {
            removeAllContainers();
        }
    }

    private static void removeAllContainers() throws IOException, InterruptedException {
        executeCommand("docker-compose rm --stop --force -v");
    }

    private static void startKafkaAsync() throws IOException, InterruptedException {
        log.info("Starting Kafka...");
        executeCommand("docker-compose up -d kafka");
    }

    private static void createTopics() throws IOException, InterruptedException {
        Map<String, String> environment = new HashMap<>();
        environment.put("KAFKA_PORT", Integer.toString(KAFKA_PORT));
        environment.put(
                "KAFKA_CREATE_TOPICS",
                format(
                        "%s:1:1,%s:%d:1,%s:%d:1",
                        TIMES_TOPIC, INPUT_TOPIC, partitions, INTERNAL_RESOLVED_EVENTS_REPARTITION_TOPIC, partitions));

        log.info("Creating topics...");
        executeCommand(createCommandToBeExecutedOnKafkaBroker("create-topics.sh", environment));
        log.info("Topics created.");
    }

    private static void populateEventsInKafka(File input) throws IOException, InterruptedException {
        log.info("Populating events in Kafka...");
        executeCommand(createCommandToBeExecutedOnKafkaBroker(format(
                "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server '%s' " +
                        "--topic '%s' --property parse.key=true --property key.separator='%s' < %s",
                BOOTSTRAP_SERVER, INPUT_TOPIC, KEY_SEPARATOR, input)));
        log.info("Events population finished.");
    }

    private static void startApp() throws IOException, InterruptedException {
        Map<String, String> environment = new HashMap<>();
        environment.put(SORT_ALGORITHM_VARIABLE, sortAlgorithm.name());
        environment.put(PARTITIONS_VARIABLE, Integer.toString(partitions));
        environment.put(INSTANCES_VARIABLE, Integer.toString(instances));

        log.info("Starting {} instances...", instances);
        executeCommand(format(
                "%s docker-compose up --scale books-app=%d books-app",
                environment.entrySet().stream()
                        .map(entry -> format("%s='%s'", entry.getKey(), entry.getValue()))
                        .collect(joining(" ")),
                instances));
        log.info("Instances' execution finished.");
    }

    private static void reportTimes() throws IOException {
        log.info("Waiting for partition processing times...");
        long averageTimeMs = waitAndGetAverageProcessingTimeForPartition().toMillis();
        log.info("Times gathered.");

        String result = Stream.of(inputOrder, duplicateInput, sortAlgorithm, partitions, instances, averageTimeMs)
                .map(Object::toString)
                .collect(joining(","));
        log.info("Results: {}", result);
        Files.writeString(TIMES_OUTPUT.toPath(), result + System.lineSeparator(), UTF_8, CREATE, APPEND);
    }

    private static Duration waitAndGetAverageProcessingTimeForPartition() throws IOException {
        return executeCommand(createCommandToBeExecutedOnKafkaBroker(
                format(
                        "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server '%s' --topic '%s' --from-beginning",
                        BOOTSTRAP_SERVER, TIMES_TOPIC)),
                output -> output.lines()
                        .limit(partitions)
                        .map(Serdes::stringToDuration)
                        .reduce(Duration.ZERO, Duration::plus)
                        .dividedBy(partitions));
    }

    private static String createCommandToBeExecutedOnKafkaBroker(String command) {
        return createCommandToBeExecutedOnKafkaBroker(command, emptyMap());
    }

    private static String createCommandToBeExecutedOnKafkaBroker(String command, Map<String, String> environment) {
        return format(
                "docker-compose exec -T %s kafka %s",
                environment.entrySet().stream()
                        .map(entry -> format("--env %s='%s'", entry.getKey(), entry.getValue()))
                        .collect(joining(" ")),
                command);
    }

    private static void executeCommand(String command) throws IOException, InterruptedException {
        Process process = createProcessBuilder(command)
                .redirectOutput(INHERIT)
                .start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException(format("Command '%s' exited with code %d.", command, exitCode));
        }
    }

    private static <T> T executeCommand(String command, Function<BufferedReader, T> outputMapper) throws IOException {
        Process process = createProcessBuilder(command)
                .redirectOutput(PIPE)
                .start();
        try (BufferedReader output = new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8))) {
            return outputMapper.apply(output);
        } finally {
            process.destroy();
        }
    }

    private static ProcessBuilder createProcessBuilder(String command) {
        return new ProcessBuilder("bash", "-l", "-c", command)
                .directory(WORK_DIR)
                .redirectError(INHERIT);
    }
}
