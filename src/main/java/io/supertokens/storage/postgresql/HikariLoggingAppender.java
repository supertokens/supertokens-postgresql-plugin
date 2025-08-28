/*
 *    Copyright (c) 2020, VRAI Labs and/or its affiliates. All rights reserved.
 *
 *    This software is licensed under the Apache License, Version 2.0 (the
 *    "License") as published by the Apache Software Foundation.
 *
 *    You may not use this file except in compliance with the License. You may
 *    obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *    License for the specific language governing permissions and limitations
 *    under the License.
 *
 */

package io.supertokens.storage.postgresql;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.core.status.Status;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.opentelemetry.OtelProvider;
import io.supertokens.storage.postgresql.output.Logging;

import java.util.List;
import java.util.Map;

public class HikariLoggingAppender implements Appender<ILoggingEvent> {

    static final String NAME = "io.supertokens.storage.postgresql.HikariLoggingAppender";

    private final Start start;

    private Context context;

    private OtelProvider otelProvider;

    HikariLoggingAppender(Start start) {
        super();
        this.start = start;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public boolean isStarted() {
        return true;
    }

    @Override
    public Context getContext() {
        return context;
    }

    @Override
    public void setContext(Context context) {
        this.context = context;
    }

    @Override
    public void addStatus(Status status) {

    }

    @Override
    public void addInfo(String msg) {
    }

    @Override
    public void addInfo(String msg, Throwable ex) {

    }

    @Override
    public void addWarn(String msg) {

    }

    @Override
    public void addWarn(String msg, Throwable ex) {

    }

    @Override
    public void addError(String msg) {
    }

    @Override
    public void addError(String msg, Throwable ex) {
    }

    @Override
    public void addFilter(Filter<ILoggingEvent> newFilter) {
    }

    @Override
    public void clearAllFilters() {
    }

    @Override
    public List<Filter<ILoggingEvent>> getCopyOfAttachedFiltersList() {
        return null;
    }

    @Override
    public FilterReply getFilterChainDecision(ILoggingEvent event) {
        return null;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void setName(String name) {
    }

    public void setOtelProvider(OtelProvider otelProvider) {
        this.otelProvider = otelProvider;
    }

    @Override
    public void doAppend(ILoggingEvent event) throws LogbackException {
        if (event.getLevel() == Level.ERROR) {
            Logging.error(start, event.getFormattedMessage(), false);
        } else if (event.getLevel() == Level.WARN) {
            Logging.warn(start, event.getFormattedMessage());
        } else {
            Logging.debug(start, event.getFormattedMessage());
        }
        appendToOtel(event);
    }

    private void appendToOtel(ILoggingEvent event) {
        if (otelProvider != null) {
            String logMessage = event.getFormattedMessage();
            String logLevel = event.getLevel().toString();
            otelProvider.createLogEvent(TenantIdentifier.BASE_TENANT, logMessage, logLevel,
                    Map.of("loggerName", event.getLoggerName(),
                            "threadName", event.getThreadName()));
        }
    }

}
