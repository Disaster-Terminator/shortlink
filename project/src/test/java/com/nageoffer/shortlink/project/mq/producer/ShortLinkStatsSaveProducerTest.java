/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this work.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nageoffer.shortlink.project.mq.producer;

import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.messaging.Message;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class ShortLinkStatsSaveProducerTest {
    @Test void eventIdIsInPayloadAndRocketMqKey(){
        RocketMQTemplate t=mock(RocketMQTemplate.class);
        SendResult r=mock(SendResult.class);
        when(r.getSendStatus()).thenReturn(SendStatus.SEND_OK);
        when(t.syncSend(eq("short-link-stats"),org.mockito.ArgumentMatchers.<Message<?>>any())).thenReturn(r);
        ShortLinkStatsSaveProducer p=new ShortLinkStatsSaveProducer(t);
        ReflectionTestUtils.setField(p,"statsTopic","short-link-stats");
        p.send(Map.of("statsRecord","{}"));
        ArgumentCaptor<Message<?>> c=ArgumentCaptor.forClass(Message.class);
        verify(t).syncSend(eq("short-link-stats"),c.capture());
        Map<?,?> payload=(Map<?,?>)c.getValue().getPayload();
        String id=(String)payload.get(ShortLinkStatsSaveProducer.EVENT_ID_FIELD);
        assertNotNull(id);
        assertEquals(id,c.getValue().getHeaders().get(RocketMQHeaders.KEYS));
    }

    @Test void nonOkSendFailsFast(){
        RocketMQTemplate t=mock(RocketMQTemplate.class);
        SendResult r=mock(SendResult.class);
        when(r.getSendStatus()).thenReturn(SendStatus.FLUSH_DISK_TIMEOUT);
        when(t.syncSend(eq("short-link-stats"),org.mockito.ArgumentMatchers.<Message<?>>any())).thenReturn(r);
        ShortLinkStatsSaveProducer p=new ShortLinkStatsSaveProducer(t);
        ReflectionTestUtils.setField(p,"statsTopic","short-link-stats");
        assertThrows(IllegalStateException.class,()->p.send(Map.of("statsRecord","{}")));
    }
}
