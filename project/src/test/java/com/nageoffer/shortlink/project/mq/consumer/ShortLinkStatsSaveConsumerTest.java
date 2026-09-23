/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this work.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nageoffer.shortlink.project.mq.consumer;

import com.alibaba.fastjson2.JSON;
import com.nageoffer.shortlink.project.common.convention.exception.StatsEventAlreadyProcessedException;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkGotoMapper;
import com.nageoffer.shortlink.project.dto.biz.ShortLinkStatsRecordDTO;
import com.nageoffer.shortlink.project.service.impl.ShortLinkStatsPersistenceService;
import org.junit.jupiter.api.*;
import org.redisson.api.RedissonClient;
import org.springframework.dao.DuplicateKeyException;
import java.util.*;
import static com.nageoffer.shortlink.project.mq.producer.ShortLinkStatsSaveProducer.EVENT_ID_FIELD;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ShortLinkStatsSaveConsumerTest {
    private ShortLinkStatsSaveConsumer consumer;
    private Map<String,String> message;

    @BeforeEach void setUp(){
        consumer=spy(new ShortLinkStatsSaveConsumer(mock(ShortLinkGotoMapper.class),mock(RedissonClient.class),mock(ShortLinkStatsPersistenceService.class)));
        message=new HashMap<>();
        message.put(EVENT_ID_FIELD,"event-1");
        message.put("statsRecord",JSON.toJSONString(ShortLinkStatsRecordDTO.builder().fullShortUrl("nurl.ink:8001/a").build()));
    }

    @Test void duplicateCommittedEventIsAcked(){
        doThrow(new StatsEventAlreadyProcessedException("event-1",new DuplicateKeyException("dup")))
                .when(consumer).actualSaveShortLinkStats(eq("event-1"),any());
        assertDoesNotThrow(()->consumer.onMessage(message));
    }

    @Test void temporaryFailureEscapesForRocketMqRetry(){
        RuntimeException failure=new RuntimeException("temporary");
        doThrow(failure).when(consumer).actualSaveShortLinkStats(eq("event-1"),any());
        assertSame(failure,assertThrows(RuntimeException.class,()->consumer.onMessage(message)));
    }

    @Test void successIsAccepted(){
        doNothing().when(consumer).actualSaveShortLinkStats(eq("event-1"),any());
        assertDoesNotThrow(()->consumer.onMessage(message));
        verify(consumer).actualSaveShortLinkStats(eq("event-1"),any());
    }
}
