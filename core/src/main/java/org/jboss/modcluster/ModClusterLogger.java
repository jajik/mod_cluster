/*
 * Copyright The mod_cluster Project Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.jboss.modcluster;

import static org.jboss.logging.Logger.Level.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.modcluster.container.Context;
import org.jboss.modcluster.container.Engine;
import org.jboss.modcluster.container.Host;
import org.jboss.modcluster.mcmp.MCMPRequestType;

/**
 * @author Paul Ferraro
 */
@MessageLogger(projectCode = "MODCLUSTER")
public interface ModClusterLogger {
    ModClusterLogger LOGGER = Logger.getMessageLogger(ModClusterLogger.class, ModClusterLogger.class.getPackage().getName());

    String CATCHING_MARKER = "Catching";

    /**
     * Logging message that logs stack trace at the DEBUG level.
     *
     * @param throwable Throwable that has been caught
     */
    @LogMessage(level = DEBUG)
    @Message(id = 0, value = CATCHING_MARKER)
    void catchingDebug(@Cause Throwable throwable);

    @LogMessage(level = INFO)
    @Message(id = 1, value = "Initializing mod_cluster version %s")
    void init(String version);

    @LogMessage(level = INFO)
    @Message(id = 2, value = "Initiating mod_cluster shutdown")
    void shutdown();

    @LogMessage(level = DEBUG)
    @Message(id = 3, value = "Received server start event")
    void startServer();

    @LogMessage(level = DEBUG)
    @Message(id = 4, value = "Received server stop event")
    void stopServer();

    @LogMessage(level = DEBUG)
    @Message(id = 5, value = "Received add context event for %s:%s")
    void addContext(Host host, Context context);

    @LogMessage(level = DEBUG)
    @Message(id = 6, value = "Received remove context event for %s:%s")
    void removeContext(Host host, Context context);

    @LogMessage(level = DEBUG)
    @Message(id = 7, value = "Received start context event for %s:%s")
    void startContext(Host host, Context context);

    @LogMessage(level = DEBUG)
    @Message(id = 8, value = "Received stop context event for %s:%s")
    void stopContext(Host host, Context context);

    @LogMessage(level = DEBUG)
    @Message(id = 9, value = "Sending %s for %s")
    void sendEngineCommand(MCMPRequestType command, Engine engine);

    @LogMessage(level = DEBUG)
    @Message(id = 10, value = "Sending %s for %s:%s")
    void sendContextCommand(MCMPRequestType command, Host host, Context context);

    @LogMessage(level = INFO)
    @Message(id = 11, value = "%s will use %s as jvm-route")
    void detectJvmRoute(Engine engine, String jvmRoute);

    @LogMessage(level = INFO)
    @Message(id = 12, value = "%s connector will use %s")
    void detectConnectorAddress(Engine engine, InetAddress address);

    @LogMessage(level = DEBUG)
    @Message(id = 20, value = "Waiting to drain %d pending requests from %s:%s")
    void drainRequests(int requests, Host host, Context context);

    @LogMessage(level = INFO)
    @Message(id = 21, value = "All pending requests drained from %s:%s in %.1f seconds")
    void requestsDrained(Host host, Context context, float seconds);

    @LogMessage(level = WARN)
    @Message(id = 22, value = "Failed to drain %d remaining pending requests from %s:%s within %.1f seconds")
    void requestDrainTimeout(int requests, Host host, Context context, float seconds);

    @LogMessage(level = DEBUG)
    @Message(id = 23, value = "Waiting to drain %d active sessions from %s:%s")
    void drainSessions(int sessions, Host host, Context context);

    @LogMessage(level = INFO)
    @Message(id = 24, value = "All active sessions drained from %s:%s in %.1f seconds")
    void sessionsDrained(Host host, Context context, float seconds);

    @LogMessage(level = WARN)
    @Message(id = 25, value = "Failed to drain %d remaining active sessions from %s:%s within %.1f seconds")
    void sessionDrainTimeout(int sessions, Host host, Context context, float seconds);

//    @LogMessage(level = WARN)
//    @Message(id = 30, value = "Attempted to bind multicast socket to a unicast address: %s.  Multicast socket will not be bound to an address.")
//    void createMulticastSocketWithUnicastAddress(InetAddress address);

    @LogMessage(level = WARN)
    @Message(id = 31, value = "Could not bind multicast socket to %s (%s address): %s; make sure your multicast address is of the same type as the IP stack (IPv4 or IPv6). Multicast socket will not be bound to an address, but this may lead to cross talking (see https://developer.jboss.org/docs/DOC-9469 for details).")
    void potentialCrossTalking(InetAddress address, String addressType, String message);

    @LogMessage(level = INFO)
    @Message(id = 32, value = "Listening to proxy advertisements on %s")
    void startAdvertise(InetSocketAddress address);

//    @LogMessage(level = WARN)
//    @Message(id = 33, value = "Failed to interrupt socket reception.")
//    void socketInterruptFailed(@Cause Throwable cause);

    @LogMessage(level = ERROR)
    @Message(id = 34, value = "Failed to start advertise listener")
    void advertiseStartFailed(@Cause Throwable cause);

    @LogMessage(level = ERROR)
    @Message(id = 40, value = "Failed to parse response header from %2$s for %1$s command")
    void parseHeaderFailed(@Cause Throwable cause, MCMPRequestType command, InetSocketAddress proxy);

    @LogMessage(level = ERROR)
    @Message(id = 41, value = "Unrecoverable syntax error %s sending %s command to %s: %s")
    void unrecoverableErrorResponse(String errorType, MCMPRequestType type, InetSocketAddress proxy, String message);

    @LogMessage(level = ERROR)
    @Message(id = 42, value = "Error %s sending %s command to %s, configuration will be reset: %s")
    void recoverableErrorResponse(String errorType, MCMPRequestType type, InetSocketAddress proxy, String message);

    @LogMessage(level = ERROR)
    @Message(id = 43, value = "Failed to send %s command to %s: %s")
    void sendFailed(MCMPRequestType type, InetSocketAddress proxy, String message);

//    @LogMessage(level = WARN)
//    @Message(id = 44, value = "%s requires com.sun.management.OperatingSystemMXBean.")
//    void missingOSBean(String classname);

    @LogMessage(level = WARN)
    @Message(id = 45, value = "%s is not supported on this system and will be disabled.")
    void notSupportedOnSystem(String classname);

    @LogMessage(level = INFO)
    @Message(id = 46, value = "Starting to drain %d active sessions from %s:%s in %d seconds.")
    void startSessionDraining(int sessions, Host host, Context context, long timeout);

//    @Message(id = 47, value = "No configured connector matches specified host:port (%s)! Ensure connectorPort and/or connectorAddress are configured.")
//    RuntimeException connectorNoMatch(String connector);

    @Message(id = 48, value = "Multiple connectors match specified host:port (%s)! Ensure connectorPort and/or connectorAddress are configured.")
    RuntimeException connectorMatchesMultiple(String connector);

    @Message(id = 49, value = "Could not resolve configured connector address (%d)!")
    RuntimeException connectorAddressUnknownHost(String connectorAddress);

    @Message(id = 50, value = "Initial load must be within the range [0..100] or -1 to not prepopulate with initial load, but was: %d")
    RuntimeException invalidInitialLoad(int initialLoad);

    @Message(id = 51, value = "No valid advertise interface configured! Disabling multicast advertise mechanism.")
    RuntimeException noValidAdvertiseInterfaceConfigured();

    @Message(id = 52, value = "Attempted to create multicast socket without multicast address specified! Disabling multicast advertise mechanism.")
    RuntimeException createMulticastSocketWithNullMulticastAddress();

    @Message(id = 53, value = "Attempted to create multicast socket with unicast address (%s)! Disabling multicast advertise mechanism.")
    RuntimeException createMulticastSocketWithUnicastAddress(InetAddress address);

    @LogMessage(level = INFO)
    @Message(id = 54, value = "Starting to drain %d active sessions from %s:%s waiting indefinitely until all remaining sessions are drained or expired.")
    void startSessionDrainingIndefinitely(int sessions, Host host, Context context);

    @LogMessage(level = WARN)
    @Message(id = 55, value = "No configured connector for engine %s. If this engine should be used with mod_cluster check connector, connectorPort and/or connectorAddress configuration.")
    void noConnectorForEngine(String engineName);
}
