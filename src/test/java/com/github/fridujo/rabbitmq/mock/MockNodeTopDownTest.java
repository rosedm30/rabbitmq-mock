package com.github.fridujo.rabbitmq.mock;

import com.github.fridujo.rabbitmq.mock.exchange.MockDefaultExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.rabbitmq.client.AMQP;
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
}