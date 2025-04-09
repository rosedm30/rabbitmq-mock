package com.github.fridujo.rabbitmq.mock;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
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
}
