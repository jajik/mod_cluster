/*
 * Copyright The mod_cluster Project Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.jboss.modcluster.advertise.impl;

import static org.jboss.modcluster.advertise.impl.AdvertiseListenerImpl.clearBuffer;
import static org.jboss.modcluster.advertise.impl.AdvertiseListenerImpl.flipBuffer;
import static org.jboss.modcluster.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.math.BigInteger;
import java.security.MessageDigest;

import org.jboss.modcluster.TestUtils;
import org.jboss.modcluster.advertise.AdvertiseListener;
import org.jboss.modcluster.advertise.DatagramChannelFactory;
import org.jboss.modcluster.config.AdvertiseConfiguration;
import org.jboss.modcluster.config.ProxyConfiguration;
import org.jboss.modcluster.mcmp.MCMPHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests {@link AdvertiseListenerImpl}.
 *
 * @author Brian Stansberry
 * @author Radoslav Husar
 */
class AdvertiseListenerImplTestCase {

    private static final String ADVERTISE_GROUP = System.getProperty("multicast.address1", "224.0.1.106");
    private static final int ADVERTISE_PORT = 23364;
    private static final String SERVER1 = "127.0.0.1";
    private static final String SERVER2 = "127.0.1.1";
    private static final int SERVER_PORT = 8989;
    private static final String SERVER1_ADDRESS = String.format("%s:%d", SERVER1, SERVER_PORT);
    private static final String SERVER2_ADDRESS = String.format("%s:%d", SERVER2, SERVER_PORT);
    private static final long TIMEOUT = 3_000; // time for advertise worker to process messages

    private MCMPHandler mcmpHandler = mock(MCMPHandler.class);
    private AdvertiseConfiguration config = mock(AdvertiseConfiguration.class);
    private DatagramChannelFactory channelFactory = mock(DatagramChannelFactory.class);

    private DatagramChannel channel;

    @BeforeEach
    void setup() throws Exception {
        when(this.config.getAdvertiseThreadFactory()).thenReturn(Executors.defaultThreadFactory());
        when(this.config.getAdvertiseSocketAddress()).thenReturn(new InetSocketAddress(ADVERTISE_GROUP, ADVERTISE_PORT));
        when(this.config.getAdvertiseSecurityKey()).thenReturn(null);
        when(this.config.getAdvertiseInterface()).thenReturn(TestUtils.getAdvertiseInterface());

        InetAddress groupAddress = InetAddress.getByName(ADVERTISE_GROUP);
        this.channel = new DatagramChannelFactoryImpl().createDatagramChannel(new InetSocketAddress(groupAddress, ADVERTISE_PORT));
    }

    @Test
    void testBasicOperation() throws Exception {
        // Test using a separate sendChannel to test AdvertiseListenerImpl
        try (DatagramChannel sendChannel = new SendingDatagramChannelFactoryImpl().createDatagramChannel(new InetSocketAddress(ADVERTISE_GROUP, ADVERTISE_PORT))) {

            // Setup ArgumentCaptor before the listener is started by AdvertiseListenerImpl constructor
            ArgumentCaptor<InetSocketAddress> capturedAddress = ArgumentCaptor.forClass(InetSocketAddress.class);
            when(this.channelFactory.createDatagramChannel(capturedAddress.capture())).thenReturn(this.channel);

            AdvertiseListener listener = new AdvertiseListenerImpl(this.mcmpHandler, this.config, this.channelFactory);

            assertEquals(ADVERTISE_GROUP, capturedAddress.getValue().getAddress().getHostAddress());
            assertTrue(this.channel.isOpen());
            assertTrue(sendChannel.isOpen());

            ArgumentCaptor<ProxyConfiguration> capturedSocketAddress = ArgumentCaptor.forClass(ProxyConfiguration.class);

            ByteBuffer buffer = ByteBuffer.allocate(512);
            buffer.put(TestUtils.generateAdvertisePacketData(new Date(), 0, SERVER1, SERVER1_ADDRESS));
            flipBuffer(buffer);

            sendChannel.send(buffer, config.getAdvertiseSocketAddress());
            flipBuffer(buffer);

            verify(this.mcmpHandler, timeout(TIMEOUT)).addProxy(capturedSocketAddress.capture());
            reset(this.mcmpHandler);

            InetSocketAddress socketAddress = capturedSocketAddress.getValue().getRemoteAddress();
            assertEquals(SERVER1, socketAddress.getAddress().getHostAddress());
            assertEquals(SERVER_PORT, socketAddress.getPort());

            capturedSocketAddress = ArgumentCaptor.forClass(ProxyConfiguration.class);
            clearBuffer(buffer);
            buffer.put(TestUtils.generateAdvertisePacketData(new Date(), 1, SERVER2, SERVER2_ADDRESS));
            flipBuffer(buffer);

            sendChannel.send(buffer, config.getAdvertiseSocketAddress());
            flipBuffer(buffer);

            verify(this.mcmpHandler, timeout(TIMEOUT)).addProxy(capturedSocketAddress.capture());
            reset(this.mcmpHandler);

            socketAddress = capturedSocketAddress.getValue().getRemoteAddress();
            assertEquals(SERVER2, socketAddress.getAddress().getHostAddress());
            assertEquals(SERVER_PORT, socketAddress.getPort());

            assertFalse(this.channel.isConnected());

            if (!System.getProperty("os.name").toLowerCase(Locale.ENGLISH).startsWith("mac")) {
                listener.close();
            } else {
                try {
                    listener.close();
                } catch (IOException e) {
                    // Workaround for https://bugs.openjdk.java.net/browse/JDK-8050499
                    if (!"Unknown error: 316".equals(e.getMessage()))
                        throw e;
                }
            }

            assertFalse(this.channel.isOpen());
        }
    }

    @Test
    void testMissingPacketData() throws Exception {
        // Test using a separate sendChannel to test AdvertiseListenerImpl
        try (DatagramChannel sendChannel = new SendingDatagramChannelFactoryImpl().createDatagramChannel(new InetSocketAddress(ADVERTISE_GROUP, ADVERTISE_PORT))) {
            // Setup ArgumentCaptor before the listener is started by AdvertiseListenerImpl constructor
            ArgumentCaptor<InetSocketAddress> capturedAddress = ArgumentCaptor.forClass(InetSocketAddress.class);
            when(this.channelFactory.createDatagramChannel(capturedAddress.capture())).thenReturn(this.channel);

            AdvertiseListener listener = new AdvertiseListenerImpl(this.mcmpHandler, this.config, this.channelFactory);

            assertEquals(ADVERTISE_GROUP, capturedAddress.getValue().getAddress().getHostAddress());
            assertTrue(this.channel.isOpen());
            assertTrue(sendChannel.isOpen());

            ArgumentCaptor<ProxyConfiguration> capturedSocketAddress = ArgumentCaptor.forClass(ProxyConfiguration.class);

            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(zeroMd5Sum);
            String rfcDate = df.format(new Date());
            digestString(md, rfcDate);
            digestString(md, String.valueOf(0));
            String server = SERVER1;
            digestString(md, server);
            BigInteger digest = new BigInteger(1, md.digest());

            byte[][] badPackets = {
                    String.format("HTTP/1.1 200 OK\r\nSequence: %d\r\nDigest: %032x\r\nServer: %s\r\nX-Manager-Address: %s\r\n", 0, digest, server, SERVER1_ADDRESS).getBytes(),
                    String.format("HTTP/1.1 200 OK\r\nDate: %s\r\nDigest: %032x\r\nServer: %s\r\nX-Manager-Address: %s\r\n", rfcDate, digest, server, SERVER1_ADDRESS).getBytes(),
                    String.format("HTTP/1.1 200 OK\r\nDate: %s\r\nSequence: %d\r\nServer: %s\r\nX-Manager-Address: %s\r\n", rfcDate, 0, server, SERVER1_ADDRESS).getBytes(),
                    String.format("HTTP/1.1 200 OK\r\nDate: %s\r\nSequence: %d\r\nDigest: %032x\r\nX-Manager-Address: %s\r\n", rfcDate, 0, digest, SERVER1_ADDRESS).getBytes(),
                    String.format("HTTP/1.1 200 OK\r\nDate: %s\r\nSequence: %d\r\nDigest: %032x\r\nServer: %s\r\n", rfcDate, 0, digest, server).getBytes()
            };

            for (int i = 0; i < badPackets.length; i++) {
                ByteBuffer buffer = ByteBuffer.allocate(512);
                buffer.put(badPackets[i]);
                flipBuffer(buffer);

                // Send a bad packet and verify it's rejected / does not have any effect
                sendChannel.send(buffer, config.getAdvertiseSocketAddress());
                verify(this.mcmpHandler, after(TIMEOUT).never()).addProxy(capturedSocketAddress.capture());

                // Now send a good packet to verify the listener is still running
                clearBuffer(buffer);
                // To prevent conflicts between iterations, we have to have a unique address
                server = String.format("127.0.%d.1", i);
                String serverAddress = String.format("%s:%d", server, SERVER_PORT);
                buffer.put(TestUtils.generateAdvertisePacketData(new Date(), 0, server, serverAddress));
                flipBuffer(buffer);
                sendChannel.send(buffer, config.getAdvertiseSocketAddress());
                verify(this.mcmpHandler, timeout(TIMEOUT)).addProxy(capturedSocketAddress.capture());
                reset(this.mcmpHandler);
            }

            assertTrue(this.channel.isOpen());

            if (!System.getProperty("os.name").toLowerCase(Locale.ENGLISH).startsWith("mac")) {
                listener.close();
            } else {
                try {
                    listener.close();
                } catch (IOException e) {
                    // Workaround for https://bugs.openjdk.java.net/browse/JDK-8050499
                    if (!"Unknown error: 316".equals(e.getMessage()))
                        throw e;
                }
            }

            assertFalse(this.channel.isOpen());
        }
    }
}
