/*
 * Copyright The mod_cluster Project Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.jboss.modcluster.mcmp.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jboss.logging.Logger;
import org.jboss.modcluster.mcmp.MCMPResponseParser;
import org.jboss.modcluster.mcmp.ResetRequestSource;
import org.jboss.modcluster.mcmp.ResetRequestSource.VirtualHost;
import org.jboss.modcluster.mcmp.impl.DefaultMCMPHandler.VirtualHostImpl;

/**
 * @author Paul Ferraro
 */
public class DefaultMCMPResponseParser implements MCMPResponseParser {
    private static final String PARAMETER_DELIMITER = "&";
    private static final String NAME_VALUE_DELIMITER = "=";

    private static final Logger log = Logger.getLogger(DefaultMCMPResponseParser.class);

    @Override
    public Map<String, Set<VirtualHost>> parseInfoResponse(String response) {
        if (response == null)
            return Collections.emptyMap();

        log.trace(response);

        // Map node id -> node name (i.e. jvm route)
        Map<String, String> nodeMap = new HashMap<String, String>();
        // Map node name -> vhost id -> virtual host
        Map<String, Map<String, ResetRequestSource.VirtualHost>> virtualHostMap = new HashMap<String, Map<String, ResetRequestSource.VirtualHost>>();

        for (String line : response.split("\r\n|\r|\n")) {
            if (line.startsWith("Node:")) {
                String[] entries = line.split(",");
                String nodeId = this.parseIds(entries[0])[0];

                // We can skip the first entry
                for (int i = 1; i < entries.length; ++i) {
                    String entry = entries[i];
                    int index = entry.indexOf(':');

                    if (index < 0) {
                        throw new IllegalArgumentException(response);
                    }

                    String key = entry.substring(0, index).trim();
                    String value = entry.substring(index + 1).trim();

                    if ("Name".equals(key)) {
                        nodeMap.put(nodeId, value);
                        virtualHostMap.put(value, new HashMap<String, ResetRequestSource.VirtualHost>());
                        break;
                    }
                }
            } else if (line.startsWith("Vhost:")) {
                String[] entries = line.split(",");
                String[] ids = this.parseIds(entries[0]);

                if (ids.length != 3) {
                    throw new IllegalArgumentException(response);
                }

                String node = nodeMap.get(ids[0]);

                if (node == null) {
                    throw new IllegalArgumentException(response);
                }

                Map<String, ResetRequestSource.VirtualHost> hostMap = virtualHostMap.get(node);
                String hostId = ids[1];

                ResetRequestSource.VirtualHost host = hostMap.get(hostId);

                if (host == null) {
                    host = new VirtualHostImpl();
                    hostMap.put(hostId, host);
                }

                for (int i = 1; i < entries.length; ++i) {
                    String entry = entries[i];
                    int index = entry.indexOf(':');

                    if (index < 0) {
                        throw new IllegalArgumentException(response);
                    }

                    String key = entry.substring(0, index).trim();
                    String value = entry.substring(index + 1).trim();

                    if ("Alias".equals(key)) {
                        host.getAliases().add(value);
                        break;
                    }
                }
            } else if (line.startsWith("Context:")) {
                String[] entries = line.split(",");
                String[] ids = this.parseIds(entries[0]);

                if (ids.length != 3) {
                    throw new IllegalArgumentException(response);
                }

                String nodeId = ids[0];
                String node = nodeMap.get(nodeId);

                if (node == null) {
                    throw new IllegalArgumentException(response);
                }

                Map<String, ResetRequestSource.VirtualHost> hostMap = virtualHostMap.get(node);
                String hostId = ids[1];

                ResetRequestSource.VirtualHost host = hostMap.get(hostId);

                if (host == null) {
                    throw new IllegalArgumentException(response);
                }

                String context = null;
                ResetRequestSource.Status status = null;

                for (int i = 1; i < entries.length; ++i) {
                    String entry = entries[i];
                    int index = entry.indexOf(':');

                    if (index < 0) {
                        throw new IllegalArgumentException(response);
                    }

                    String key = entry.substring(0, index).trim();
                    String value = entry.substring(index + 1).trim();

                    if ("Context".equals(key)) {
                        context = value;
                    } else if ("Status".equals(key)) {
                        status = ResetRequestSource.Status.valueOf(value);
                    }
                }

                if ((context == null) || (status == null)) {
                    throw new IllegalArgumentException(response);
                }

                host.getContexts().put(context, status);
            }
        }

        Map<String, Set<ResetRequestSource.VirtualHost>> result = new HashMap<String, Set<ResetRequestSource.VirtualHost>>();

        for (Map.Entry<String, Map<String, ResetRequestSource.VirtualHost>> entry : virtualHostMap.entrySet()) {
            result.put(entry.getKey(), new HashSet<ResetRequestSource.VirtualHost>(entry.getValue().values()));
        }

        log.trace(result);

        return result;
    }

    private String[] parseIds(String entry) {
        int start = entry.indexOf('[') + 1;
        int end = entry.indexOf(']');

        if (start >= end) {
            throw new IllegalArgumentException(entry);
        }

        String ids = entry.substring(start, end);

        return (ids.length() > 2) ? ids.split(":") : new String[] { ids };
    }

    @Override
    public boolean parsePingResponse(String response) {
        log.trace(response);

        String value = this.findProperty("State", response);

        return (value != null) ? value.equals("OK") : false;
    }

    @Override
    public int parseStopAppResponse(String response) {
        log.trace(response);

        String value = this.findProperty("Requests", response);

        try {
            return (value != null) ? Integer.parseInt(value) : 0;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private String findProperty(String name, String response) {
        if (response == null)
            return null;

        for (String value : response.trim().split(PARAMETER_DELIMITER)) {
            String[] pair = value.split(NAME_VALUE_DELIMITER);

            if ((pair.length == 2) && pair[0].equals(name)) {
                return pair[1];
            }
        }

        return null;
    }
}
