package com.github.fridujo.rabbitmq.mock;

import com.github.fridujo.rabbitmq.mock.exchange.MockDefaultExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.rabbitmq.client.AMQP;

import static org.junit.jupiter.api.Assertions.*;
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

    @Test
    void exchangeDeclareUsingValidAndInvalidTypes() {
        String exchangeName = "test.exchange";
        String validType = "direct";
        AMQP.Exchange.DeclareOk declareOk = mockNode.exchangeDeclare(exchangeName, validType, false, false, false,
                Collections.emptyMap());
        assertNotNull(declareOk, "Exchange with valid type should pass");

        String queueName = "test.queue";
        mockNode.queueDeclare(queueName, false, false, false, Collections.emptyMap());
        mockNode.queueBind(queueName, exchangeName, "test.routing.key", Collections.emptyMap());
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().build();
        boolean publishResult = mockNode.basicPublish(exchangeName, "test.routing.key", false, false, props,
                "Test".getBytes());
        assertTrue(publishResult, "Able to publish to exchange");
        assertEquals(1, mockNode.messageCount(queueName), "Message sent to queue by exchange");

        String invalidType = "invalid-type";
        assertThrows(
                IllegalArgumentException.class, () -> mockNode.exchangeDeclare(exchangeName + ".invalid", invalidType,
                        false, false, false, Collections.emptyMap()),
                "Exchange w/ invalid type should throw an IllegalArgumentException");
    }

    @Test
    void queueDeleteUsingValidAndInvalidQueues() {
        String queueName = "delete.test.queue";
        mockNode.queueDeclare(queueName, false, false, false, Collections.emptyMap());
        AMQP.Queue.DeleteOk deleteOk = mockNode.queueDelete(queueName, false, false);
        assertEquals(0, deleteOk.getMessageCount(), "Deleting queue with 0 messages should return 0");
        assertThrows(IllegalArgumentException.class, () -> mockNode.messageCount(queueName),
                "Queue should be gone after deletion");

        String queueNameMessage = "delete.test.queue.message";
        mockNode.queueDeclare(queueNameMessage, false, false, false, Collections.emptyMap());
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().build();
        mockNode.basicPublish(MockDefaultExchange.NAME, queueNameMessage, false, false, props, "Test".getBytes());
        deleteOk = mockNode.queueDelete(queueNameMessage, false, false);
        assertEquals(1, deleteOk.getMessageCount(), "Deleting a queue with 1 message should return 1");
        assertThrows(IllegalArgumentException.class, () -> mockNode.messageCount(queueNameMessage),
                "Queue should be gone after deletion");

        String noQueue = "no.queue";
        deleteOk = mockNode.queueDelete(noQueue, false, false);
        assertEquals(0, deleteOk.getMessageCount(), "Deleting a non existent queue should return 0 messages");
    }
}