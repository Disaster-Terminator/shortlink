/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this work.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nageoffer.shortlink.project.service.impl;

import com.nageoffer.shortlink.project.config.GotoDomainWhiteListConfiguration;
import com.nageoffer.shortlink.project.dao.entity.*;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkGotoMapper;
import com.nageoffer.shortlink.project.dto.req.ShortLinkCreateReqDTO;
import com.nageoffer.shortlink.project.mq.producer.ShortLinkStatsSaveProducer;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.redisson.api.*;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.*;
import org.springframework.test.util.ReflectionTestUtils;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ShortLinkServiceImplTest {
    private HttpServer server;
    private RBloomFilter<String> bloom;
    private ShortLinkCreatePersistenceService persistence;
    private ShortLinkServiceImpl service;
    private ValueOperations<String,String> values;
    private String origin;

    @BeforeEach void setUp() throws Exception {
        server=HttpServer.create(new InetSocketAddress(0),0);
        server.createContext("/",e->{byte[] b="<html><head></head><body>ok</body></html>".getBytes(StandardCharsets.UTF_8);e.sendResponseHeaders(200,b.length);e.getResponseBody().write(b);e.close();});
        server.start();
        origin="http://127.0.0.1:"+server.getAddress().getPort()+"/";
        bloom=mock(RBloomFilter.class);
        persistence=mock(ShortLinkCreatePersistenceService.class);
        StringRedisTemplate redis=mock(StringRedisTemplate.class);
        values=mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        GotoDomainWhiteListConfiguration white=mock(GotoDomainWhiteListConfiguration.class);
        when(white.getEnable()).thenReturn(false);
        when(bloom.contains(anyString())).thenReturn(false);
        service=new ShortLinkServiceImpl(bloom,mock(ShortLinkGotoMapper.class),redis,mock(RedissonClient.class),
                mock(ShortLinkStatsSaveProducer.class),white,persistence);
        ReflectionTestUtils.setField(service,"createShortLinkDefaultDomain","nurl.ink:8001");
    }
    @AfterEach void tearDown(){server.stop(0);}

    private ShortLinkCreateReqDTO req(){return ShortLinkCreateReqDTO.builder().originUrl(origin).gid("default").createdType(0).validDateType(0).describe("test").build();}

    @Test void retriesWithNewCandidateAfterDatabaseCollision() {
        doThrow(new DuplicateKeyException("collision")).doNothing().when(persistence).persist(any(),any());
        service.createShortLink(req());
        ArgumentCaptor<ShortLinkDO> c=ArgumentCaptor.forClass(ShortLinkDO.class);
        verify(persistence,times(2)).persist(c.capture(),any(ShortLinkGotoDO.class));
        assertNotEquals(c.getAllValues().get(0).getFullShortUrl(),c.getAllValues().get(1).getFullShortUrl());
        verify(values).set(anyString(),eq(origin),anyLong(),eq(TimeUnit.MILLISECONDS));
    }

    @Test void fallsBackToDatabaseWhenBloomFilterIsUnavailable() {
        when(bloom.contains(anyString())).thenThrow(new RuntimeException("redis unavailable"));
        service.createShortLink(req());
        verify(persistence).persist(any(),any());
    }
}
