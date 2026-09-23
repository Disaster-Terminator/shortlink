/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this work.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nageoffer.shortlink.project.service.impl;

import com.nageoffer.shortlink.project.common.convention.exception.StatsEventAlreadyProcessedException;
import com.nageoffer.shortlink.project.dao.entity.LinkAccessLogsDO;
import com.nageoffer.shortlink.project.dao.mapper.*;
import com.nageoffer.shortlink.project.dto.biz.ShortLinkStatsRecordDTO;
import org.junit.jupiter.api.*;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.embedded.*;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import javax.sql.DataSource;
import java.util.Date;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ShortLinkStatsPersistenceServiceTest {
    private AnnotationConfigApplicationContext ctx;
    private JdbcTemplate jdbc;
    private ShortLinkStatsPersistenceService service;
    private LinkOsStatsMapper os;

    @BeforeEach void setUp() {
        ctx=new AnnotationConfigApplicationContext(Config.class);
        jdbc=ctx.getBean(JdbcTemplate.class);
        service=ctx.getBean(ShortLinkStatsPersistenceService.class);
        os=ctx.getBean(LinkOsStatsMapper.class);
        jdbc.execute("CREATE TABLE stats_event(event_id VARCHAR(64) PRIMARY KEY)");
        jdbc.execute("CREATE TABLE stats_counter(id INT PRIMARY KEY,value INT NOT NULL)");
        jdbc.update("INSERT INTO stats_counter VALUES(1,0)");
    }
    @AfterEach void tearDown(){ctx.close();}

    @Test void rollbackMakesFailureRetryableAndCommittedEventIsDeduplicated() {
        doThrow(new RuntimeException("temporary")).doNothing().when(os).shortLinkOsState(any());
        ShortLinkStatsRecordDTO r=ShortLinkStatsRecordDTO.builder()
                .fullShortUrl("nurl.ink:8001/a").uv("u").remoteAddr("127.0.0.1")
                .browser("Chrome").os("Windows").network("WIFI").device("PC")
                .currentDate(new Date()).uvFirstFlag(false).uipFirstFlag(false).build();

        assertThrows(RuntimeException.class,()->persist("event-1",r));
        assertEquals(0,eventCount());
        assertEquals(0,counter());

        persist("event-1",r);
        assertEquals(1,eventCount());
        assertEquals(1,counter());

        assertThrows(StatsEventAlreadyProcessedException.class,()->persist("event-1",r));
        assertEquals(1,counter());
    }

    private void persist(String id,ShortLinkStatsRecordDTO r){service.persist(id,"default",r,"未知","未知","未知",false);}
    private int eventCount(){return jdbc.queryForObject("SELECT COUNT(*) FROM stats_event",Integer.class);}
    private int counter(){return jdbc.queryForObject("SELECT value FROM stats_counter WHERE id=1",Integer.class);}

    @Configuration @EnableTransactionManagement
    static class Config {
        @Bean DataSource dataSource(){return new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2).build();}
        @Bean JdbcTemplate jdbcTemplate(DataSource d){return new JdbcTemplate(d);}
        @Bean PlatformTransactionManager transactionManager(DataSource d){return new DataSourceTransactionManager(d);}
        @Bean LinkAccessLogsMapper logs(JdbcTemplate j){
            LinkAccessLogsMapper m=mock(LinkAccessLogsMapper.class);
            when(m.insert(any())).thenAnswer(i->j.update("INSERT INTO stats_event VALUES(?)",((LinkAccessLogsDO)i.getArgument(0)).getEventId()));
            return m;
        }
        @Bean LinkAccessStatsMapper access(JdbcTemplate j){
            LinkAccessStatsMapper m=mock(LinkAccessStatsMapper.class);
            doAnswer(i->{j.update("UPDATE stats_counter SET value=value+1 WHERE id=1");return null;}).when(m).shortLinkStats(any());
            return m;
        }
        @Bean LinkLocaleStatsMapper locale(){return mock(LinkLocaleStatsMapper.class);}
        @Bean LinkOsStatsMapper os(){return mock(LinkOsStatsMapper.class);}
        @Bean LinkBrowserStatsMapper browser(){return mock(LinkBrowserStatsMapper.class);}
        @Bean LinkDeviceStatsMapper device(){return mock(LinkDeviceStatsMapper.class);}
        @Bean LinkNetworkStatsMapper network(){return mock(LinkNetworkStatsMapper.class);}
        @Bean LinkStatsTodayMapper today(){return mock(LinkStatsTodayMapper.class);}
        @Bean ShortLinkMapper shortLink(){return mock(ShortLinkMapper.class);}
        @Bean ShortLinkStatsPersistenceService service(LinkAccessStatsMapper a,LinkLocaleStatsMapper l,LinkOsStatsMapper o,
                LinkBrowserStatsMapper b,LinkAccessLogsMapper logs,LinkDeviceStatsMapper d,LinkNetworkStatsMapper n,
                LinkStatsTodayMapper t,ShortLinkMapper s){
            return new ShortLinkStatsPersistenceService(a,l,o,b,logs,d,n,t,s);
        }
    }
}
