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

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.Week;
import cn.hutool.core.util.StrUtil;
import com.nageoffer.shortlink.project.common.convention.exception.StatsEventAlreadyProcessedException;
import com.nageoffer.shortlink.project.dao.entity.LinkAccessLogsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkAccessStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkBrowserStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkDeviceStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkLocaleStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkNetworkStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkOsStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkStatsTodayDO;
import com.nageoffer.shortlink.project.dao.mapper.LinkAccessLogsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkAccessStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkBrowserStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkDeviceStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkLocaleStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkNetworkStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkOsStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkStatsTodayMapper;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkMapper;
import com.nageoffer.shortlink.project.dto.biz.ShortLinkStatsRecordDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;

@Service
@RequiredArgsConstructor
public class ShortLinkStatsPersistenceService {

    private final LinkAccessStatsMapper linkAccessStatsMapper;
    private final LinkLocaleStatsMapper linkLocaleStatsMapper;
    private final LinkOsStatsMapper linkOsStatsMapper;
    private final LinkBrowserStatsMapper linkBrowserStatsMapper;
    private final LinkAccessLogsMapper linkAccessLogsMapper;
    private final LinkDeviceStatsMapper linkDeviceStatsMapper;
    private final LinkNetworkStatsMapper linkNetworkStatsMapper;
    private final LinkStatsTodayMapper linkStatsTodayMapper;
    private final ShortLinkMapper shortLinkMapper;

    @Transactional(rollbackFor = Exception.class)
    public void persist(String eventId, String gid, ShortLinkStatsRecordDTO statsRecord,
                        String province, String city, String adcode, boolean localeResolved) {
        String fullShortUrl = statsRecord.getFullShortUrl();
        Date currentDate = statsRecord.getCurrentDate();

        LinkAccessLogsDO linkAccessLogsDO = LinkAccessLogsDO.builder()
                .eventId(eventId)
                .user(statsRecord.getUv())
                .ip(statsRecord.getRemoteAddr())
                .browser(statsRecord.getBrowser())
                .os(statsRecord.getOs())
                .network(statsRecord.getNetwork())
                .device(statsRecord.getDevice())
                .locale(StrUtil.join("-", "中国", province, city))
                .fullShortUrl(fullShortUrl)
                .build();
        try {
            linkAccessLogsMapper.insert(linkAccessLogsDO);
        } catch (DuplicateKeyException ex) {
            throw new StatsEventAlreadyProcessedException(eventId, ex);
        }

        int hour = DateUtil.hour(currentDate, true);
        Week week = DateUtil.dayOfWeekEnum(currentDate);
        int weekValue = week.getIso8601Value();
        linkAccessStatsMapper.shortLinkStats(LinkAccessStatsDO.builder()
                .pv(1)
                .uv(statsRecord.getUvFirstFlag() ? 1 : 0)
                .uip(statsRecord.getUipFirstFlag() ? 1 : 0)
                .hour(hour)
                .weekday(weekValue)
                .fullShortUrl(fullShortUrl)
                .date(currentDate)
                .build());

        if (localeResolved) {
            linkLocaleStatsMapper.shortLinkLocaleState(LinkLocaleStatsDO.builder()
                    .province(province).city(city).adcode(adcode).cnt(1)
                    .fullShortUrl(fullShortUrl).country("中国").date(currentDate).build());
        }
        linkOsStatsMapper.shortLinkOsState(LinkOsStatsDO.builder()
                .os(statsRecord.getOs()).cnt(1).fullShortUrl(fullShortUrl).date(currentDate).build());
        linkBrowserStatsMapper.shortLinkBrowserState(LinkBrowserStatsDO.builder()
                .browser(statsRecord.getBrowser()).cnt(1).fullShortUrl(fullShortUrl).date(currentDate).build());
        linkDeviceStatsMapper.shortLinkDeviceState(LinkDeviceStatsDO.builder()
                .device(statsRecord.getDevice()).cnt(1).fullShortUrl(fullShortUrl).date(currentDate).build());
        linkNetworkStatsMapper.shortLinkNetworkState(LinkNetworkStatsDO.builder()
                .network(statsRecord.getNetwork()).cnt(1).fullShortUrl(fullShortUrl).date(currentDate).build());
        shortLinkMapper.incrementStats(gid, fullShortUrl, 1,
                statsRecord.getUvFirstFlag() ? 1 : 0,
                statsRecord.getUipFirstFlag() ? 1 : 0);
        linkStatsTodayMapper.shortLinkTodayState(LinkStatsTodayDO.builder()
                .todayPv(1)
                .todayUv(statsRecord.getUvFirstFlag() ? 1 : 0)
                .todayUip(statsRecord.getUipFirstFlag() ? 1 : 0)
                .fullShortUrl(fullShortUrl)
                .date(currentDate)
                .build());
    }
}
