/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nageoffer.shortlink.project.service.impl;

import com.nageoffer.shortlink.project.dao.entity.ShortLinkDO;
import com.nageoffer.shortlink.project.dao.entity.ShortLinkGotoDO;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkGotoMapper;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkMapper;
import org.junit.jupiter.api.*;
import org.springframework.context.annotation.*;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.embedded.*;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import javax.sql.DataSource;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ShortLinkCreatePersistenceServiceTest {
    private AnnotationConfigApplicationContext ctx;
    private JdbcTemplate jdbc;
    private ShortLinkCreatePersistenceService service;

    @BeforeEach void setUp() {
        ctx=new AnnotationConfigApplicationContext(Config.class);
        jdbc=ctx.getBean(JdbcTemplate.class);
        service=ctx.getBean(ShortLinkCreatePersistenceService.class);
        jdbc.execute("CREATE TABLE t_link(full_short_url VARCHAR(128) PRIMARY KEY)");
        jdbc.execute("CREATE TABLE t_link_goto(full_short_url VARCHAR(128) PRIMARY KEY)");
    }
    @AfterEach void tearDown(){ctx.close();}

    @Test void rollsBackFirstInsertWhenGlobalGotoConstraintRejectsCandidate() {
        String u="nurl.ink:8001/collision";
        jdbc.update("INSERT INTO t_link_goto VALUES (?)",u);
        assertThrows(DuplicateKeyException.class,()->service.persist(
                ShortLinkDO.builder().fullShortUrl(u).build(),
                ShortLinkGotoDO.builder().fullShortUrl(u).build()));
        assertEquals(0,jdbc.queryForObject("SELECT COUNT(*) FROM t_link WHERE full_short_url=?",Integer.class,u));
        assertEquals(1,jdbc.queryForObject("SELECT COUNT(*) FROM t_link_goto WHERE full_short_url=?",Integer.class,u));
    }

    @Configuration @EnableTransactionManagement
    static class Config {
        @Bean DataSource dataSource(){return new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2).build();}
        @Bean JdbcTemplate jdbcTemplate(DataSource d){return new JdbcTemplate(d);}
        @Bean PlatformTransactionManager transactionManager(DataSource d){return new DataSourceTransactionManager(d);}
        @Bean ShortLinkMapper shortLinkMapper(JdbcTemplate j){
            ShortLinkMapper m=mock(ShortLinkMapper.class);
            when(m.insert(any())).thenAnswer(i->j.update("INSERT INTO t_link VALUES (?)",((ShortLinkDO)i.getArgument(0)).getFullShortUrl()));
            return m;
        }
        @Bean ShortLinkGotoMapper shortLinkGotoMapper(JdbcTemplate j){
            ShortLinkGotoMapper m=mock(ShortLinkGotoMapper.class);
            when(m.insert(any())).thenAnswer(i->j.update("INSERT INTO t_link_goto VALUES (?)",((ShortLinkGotoDO)i.getArgument(0)).getFullShortUrl()));
            return m;
        }
        @Bean ShortLinkCreatePersistenceService service(ShortLinkMapper a,ShortLinkGotoMapper b){return new ShortLinkCreatePersistenceService(a,b);}
    }
}
