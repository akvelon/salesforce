/*
 * Copyright 2019 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.salesforce.plugin.source.streaming;

import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSession;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * The Bayeux extension for Replaying Events.
 *
 * A subscriber can choose which events to receive,
 * such as all events within the retention window or starting after a particular event.
 * The default is to receive only the new events sent after subscribing.
 * Events outside the retention period are discarded.
 */
public class ReplayExtension implements ClientSession.Extension {

  private static final String EXTENSION_NAME = "replay";
  private static final String EVENT_KEY = "event";
  private static final String REPLAY_ID_KEY = "replayId";

  private final ConcurrentMap<String, Long> dataMap;
  private final AtomicBoolean supported = new AtomicBoolean();

  public ReplayExtension(ConcurrentMap<String, Long> dataMap) {
    this.dataMap = dataMap;
  }

  @Override
  public boolean rcv(ClientSession session, Message.Mutable message) {
    Long replayId = getReplayId(message);
    if (this.supported.get() && replayId != null) {
      try {
        String channel = topicWithoutQueryString(message.getChannel());
        dataMap.put(channel, replayId);
      } catch (ClassCastException e) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean rcvMeta(ClientSession session, Message.Mutable message) {
    if (Channel.META_HANDSHAKE.equals(message.getChannel())) {
      Map<String, Object> ext = message.getExt(false);
      this.supported.set(ext != null && Boolean.TRUE.equals(ext.get(EXTENSION_NAME)));
    }
    return true;
  }

  @Override
  public boolean sendMeta(ClientSession session, Message.Mutable message) {
    switch (message.getChannel()) {
      case Channel.META_HANDSHAKE:
        message.getExt(true).put(EXTENSION_NAME, Boolean.TRUE);
        break;
      case Channel.META_SUBSCRIBE:
        if (supported.get()) {
          message.getExt(true).put(EXTENSION_NAME, dataMap);
        }
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  private static Long getReplayId(Message.Mutable message) {
    Map<String, Object> data = message.getDataAsMap();
    Optional<Long> optional = resolve(() -> {
      Map<String, Object> eventMap = (Map<String, Object>) data.get(EVENT_KEY);
      Object eventId = eventMap.get(REPLAY_ID_KEY);

      if (eventId == null) {
        return null;
      }

      if (eventId instanceof Integer) {
        return ((Integer) eventId).longValue();
      } else if (eventId instanceof Double) {
        return ((Double) eventId).longValue();
      } else {
        return (Long) eventId;
      }
    });
    return optional.orElse(null);
  }

  private static <T> Optional<T> resolve(Supplier<T> resolver) {
    try {
      T result = resolver.get();
      return Optional.ofNullable(result);
    } catch (NullPointerException e) {
      return Optional.empty();
    }
  }

  private static String topicWithoutQueryString(String fullTopic) {
    return fullTopic.split("\\?")[0];
  }
}
