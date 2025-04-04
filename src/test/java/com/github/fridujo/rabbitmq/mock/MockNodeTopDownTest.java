package com.github.fridujo.rabbitmq.mock;

import com.github.fridujo.rabbitmq.mock.exchange.MockDefaultExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.rabbitmq.client.AMQP;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.Collections;

public class MockNodeTopDownTest {
    private MockNode mockNode;

    @BeforeEach
    void setUp() {
        mockNode = new MockNode();
        mockNode.queueDeclare("test.routing.key", false, false, false, Collections.emptyMap());
    }

    @Test
    void basicPublishUsingValidExchange() {
        String exchangeName = MockDefaultExchange.NAME;
        String routingKey = "test.routing.key";
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().build();
        byte[] body = "HELLO".getBytes();

        boolean result = mockNode.basicPublish(exchangeName, routingKey, false, false, props, body);

        assertTrue(result, "Message is successfully published to the default exchange");
    }

    @Test
    void queuePurgeUsingMessageCounts() {
        // 0 messages
        String name = "purge.test.queue";
        mockNode.queueDeclare(name, false, false, false, Collections.emptyMap());
        AMQP.Queue.PurgeOk purgeOk = mockNode.queuePurge(name);
        assertEquals(0, purgeOk.getMessageCount(), "Purge empty queue should return 0 messages removed");

        // 1 message
        mockNode.queueDeclare(name, false, false, false, Collections.emptyMap());
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().build();
        mockNode.basicPublish(MockDefaultExchange.NAME, name, false, false, props, "Message".getBytes());
        purgeOk = mockNode.queuePurge(name);
        assertEquals(1, purgeOk.getMessageCount(), "Purge 1 message should return 1 message removed");
    }
}