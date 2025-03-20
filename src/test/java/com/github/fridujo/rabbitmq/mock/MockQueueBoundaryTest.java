package com.github.fridujo.rabbitmq.mock;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import com.rabbitmq.client.AMQP;
import java.nio.charset.StandardCharsets;
import java.util.Map;

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

        System.out.println("Total messages accepted: " + acceptedMessages);
        System.out.println("Queue full status before overflow: " + (acceptedMessages >= 1000));

        boolean overflowResults = queue.publish("testing", "testKey", new AMQP.BasicProperties(),
                "Overflow Error Message".getBytes(StandardCharsets.UTF_8));

        System.out.println("Overflow publish result: " + overflowResults);

        if (messageCount < 1000) {
            assertTrue(overflowResults, "Queue should accept messages within the limit.");
        } else {
            assertFalse(overflowResults, "Queue should decline excess messages.");
        }

    }
}
