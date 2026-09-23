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

package com.nageoffer.shortlink.project.mq.producer;

import cn.hutool.core.lang.UUID;
import lombok.RequiredArgsConstructor;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class ShortLinkStatsSaveProducer {

    public static final String EVENT_ID_FIELD = "eventId";

    private final RocketMQTemplate rocketMQTemplate;

    @Value("${short-link.stats.mq.topic}")
    private String statsTopic;

    public void send(Map<String, String> producerMap) {
        String eventId = UUID.fastUUID().toString(true);
        Map<String, String> payload = new HashMap<>(producerMap);
        payload.put(EVENT_ID_FIELD, eventId);
        Message<Map<String, String>> message = MessageBuilder.withPayload(payload)
                .setHeader(RocketMQHeaders.KEYS, eventId)
                .build();
        SendResult sendResult = rocketMQTemplate.syncSend(statsTopic, message);
        if (sendResult == null || sendResult.getSendStatus() != SendStatus.SEND_OK) {
            throw new IllegalStateException("RocketMQ message send failed, eventId=" + eventId);
        }
    }
}
