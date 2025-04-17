package com.github.fridujo.rabbitmq.mock;

import static org.assertj.core.api.Assertions.offset;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import com.rabbitmq.client.AMQP;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import com.rabbitmq.client.GetResponse;
import java.util.function.Supplier;

// Dana's test cases

class MockQueueBoundaryTest {
    private MockQueue queue;

    @BeforeEach
    void setUp() {
        AmqArguments args = new AmqArguments(Map.of(
                AmqArguments.QUEUE_MAX_LENGTH_KEY, 1000,
                AmqArguments.OVERFLOW_KEY, "reject-publish"));
        System.out.println("Queue arguments: " + args);
        ReceiverRegistry registry = new MockNode();
        queue = new MockQueue("testQueue", args, registry);
    }

    @ParameterizedTest
    @ValueSource(ints = { 999, 1000, 1001 })
    void excessMessagesNotAcceptedInQueue(int messageCount) {
        int acceptedMessages = 0;

        for (int i = 0; i < messageCount; i++) {
            boolean accepted = queue.publish("testing", "testKey", new AMQP.BasicProperties(),
                    ("Message sent " + i).getBytes(StandardCharsets.UTF_8));

            if (accepted) {
                acceptedMessages++;
            } else {
                System.out.println("Message rejected at index: " + i);
                break;
            }
        }

        // System.out.println("Total messages accepted: " + acceptedMessages);
        // System.out.println("Queue full status before overflow: " + (acceptedMessages
        // >= 1000));

        boolean overflowResults = queue.publish("testing", "testKey", new AMQP.BasicProperties(),
                "Overflow Error Message".getBytes(StandardCharsets.UTF_8));

        System.out.println("Overflow publish result: " + overflowResults);

        if (messageCount < 1000) {
            assertTrue(overflowResults, "Queue should accept messages within the limit.");
        } else {
            assertFalse(overflowResults, "Queue should decline excess messages.");
        }

    }

    @ParameterizedTest
    @ValueSource(ints = { 100, 500, 750 })
    void queueAcceptsMessagesUnderLimit(int messageCount) {
        int acceptedMessages = 0;

        for (int i = 0; i < messageCount; i++) {
            boolean accepted = queue.publish("testing", "testKey", new AMQP.BasicProperties(),
                    ("Message sent " + i).getBytes(StandardCharsets.UTF_8));

            assertTrue(accepted, "Queue should accept messages when under the limit.");
            if (accepted)
                acceptedMessages++;
        }

        System.out.println("Total messages accepted: " + acceptedMessages);
        assertEquals(messageCount, acceptedMessages, "Queue should accept exactly " + messageCount + " messages.");
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 1000 })
    void basicGetWithBoundaryMessageCounts(int messageCount) {
        Supplier<Long> deliveryTagSupplier = () -> 1L;
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().build();
        for (int i = 0; i < messageCount; i++) {
            boolean accepted = queue.publish("testing", "testKey" + i, props,
                    ("Message " + i).getBytes(StandardCharsets.UTF_8));
            assertTrue(accepted, "Queue should accept message " + i);
        }
        assertEquals(messageCount, queue.messageCount(),
                "Queue should contain " + messageCount + " messages before get");

        GetResponse getResponse = queue.basicGet(true, deliveryTagSupplier);

        if (messageCount == 0) {
            assertNull(getResponse, "basicGet should return null when queue is empty");
            assertEquals(0, queue.messageCount(), "Message count should be 0 after basicGet is used on an empty queue");
        } else {
            assertNotNull(getResponse,
                    "basicGet should return a message when the queue has " + messageCount + " messages");
            assertEquals("Message 0", new String(getResponse.getBody(), StandardCharsets.UTF_8),
                    "basicGet should return the first message");
            assertEquals(messageCount - 1, queue.messageCount(),
                    "Message coutn should decrese by 1 after using basicGet");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1023, 1024, 1025 })
    void messageSize(int messageSize) {
        AmqArguments args = new AmqArguments(
                Map.of(AmqArguments.QUEUE_MAX_LENGTH_BYTES_KEY, 1024, AmqArguments.OVERFLOW_KEY, "reject-publish"));

        System.out.println("Byte limit argument set: " + args.queueLengthBytesLimit());

        ReceiverRegistry registry = new MockNode();
        MockQueue byteQueue = new MockQueue("byteQueue", args, registry);

        byte[] body = new byte[messageSize];
        boolean accepted = byteQueue.publish("testing", "key", new AMQP.BasicProperties(), body);

        if (messageSize <= 1024) {
            assertTrue(accepted, "Message under or equal to the limit should be accepted");
        } else {
            assertFalse(accepted, "Message over limit should denied");
        }
    }

    @ParameterizedTest
    @CsvSource({
            "true, true",
            "true, false",
            "false, true",
            "false, false"
    })
    void rejectUsingRequeueBehavior(boolean hasValidDeliveryTag, boolean requeue) {
        MockNode node = new MockNode();
        node.exchangeDeclare("dlx", "direct", false, false, false, Map.of());
        node.queueDeclare("dlq", false, false, false, Map.of());
        node.queueBind("dlq", "dlx", "", Map.of());

        AmqArguments args = new AmqArguments(Map.of(
                AmqArguments.DEAD_LETTER_EXCHANGE_KEY, "dlx",
                "x-dead-letter-routing-key", "",
                AmqArguments.QUEUE_MAX_LENGTH_KEY, 1000,
                AmqArguments.OVERFLOW_KEY, "reject-publish"));
        MockQueue queue = new MockQueue("testQueue", args, node);
        MockQueue dlq = node.getQueue("dlq")
                .orElseThrow(() -> new IllegalStateException("Dead-letter queue not found"));

        final long[] deliveryTagCounter = { 1L };
        Supplier<Long> deliveryTagSupplier = () -> deliveryTagCounter[0]++;
        long testDeliveryTag = 1L;
        if (hasValidDeliveryTag) {
            byte[] body = new byte[10];
            boolean published = queue.publish("testing", "key", new AMQP.BasicProperties(), body);
            assertTrue(published, "Message is pubilshed correctly");
            GetResponse response = queue.basicGet(false, deliveryTagSupplier);
            assertNotNull(response, "basicGet should return message");
            assertEquals(1, queue.getUnackedMessages().size(), "Message should be unacked after using basicGet");
        }

        long deliveryTagUsed = hasValidDeliveryTag ? testDeliveryTag : 999L;
        queue.basicReject(deliveryTagUsed, requeue);

        if (!hasValidDeliveryTag) {
            assertEquals(0, queue.messageCount(), "Queue should stay empty with invalid delivery tag");
            assertEquals(0, queue.getUnackedMessages().size(), "No unacked messages should be present with invalid");
            assertEquals(0, dlq.messageCount(), "Dead-letter queue should stay empty with invalid delivery tag");
        } else {
            assertEquals(0, queue.getUnackedMessages().size(), "Message should be removed from unacked message");
            if (requeue) {
                assertEquals(1, queue.messageCount(), "Message should be requeued when requeue is true");
                assertEquals(0, dlq.messageCount(), "Dead-letter queue should stay empty when requeue is true");
            } else {
                assertEquals(0, queue.messageCount(), "Message shouldn't be requeued when requeue is false");
                assertEquals(1, dlq.messageCount(), "Message should be dead-lettered when requeue is false");
            }
        }
    }
}
