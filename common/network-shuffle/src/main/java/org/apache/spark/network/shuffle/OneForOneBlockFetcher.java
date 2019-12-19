/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.FetchShuffleBlocks;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.shuffle.protocol.StreamHandle;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Simple wrapper on top of a TransportClient which interprets each chunk as a whole block, and
 * invokes the BlockFetchingListener appropriately. This class is agnostic to the actual RPC
 * handler, as long as there is a single "open blocks" message which returns a ShuffleStreamHandle,
 * and Java serialization is used.
 * <p>
 * Note that this typically corresponds to a
 * {@link org.apache.spark.network.server.OneForOneStreamManager} on the server side.
 */
public class OneForOneBlockFetcher {
    private static final Logger logger = LoggerFactory.getLogger(OneForOneBlockFetcher.class);

    //请求服务的client
    private final TransportClient client;

    //即OpenBlocks 携带appId/executorId/blockIds
    private final BlockTransferMessage message;

    //BlockId数组
    private final String[] blockIds;

    //BlockFetchingListener
    private final BlockFetchingListener listener;

    //回调配合BlockFetchingListener使用
    private final ChunkReceivedCallback chunkCallback;
    private final TransportConf transportConf;
    private final DownloadFileManager downloadFileManager;

    //封装服务端返回的消息
    private StreamHandle streamHandle = null;

    public OneForOneBlockFetcher(
            TransportClient client,
            String appId,
            String execId,
            String[] blockIds,
            BlockFetchingListener listener,
            TransportConf transportConf) {
        this(client, appId, execId, blockIds, listener, transportConf, null);
    }

    public OneForOneBlockFetcher(
            TransportClient client,
            String appId,
            String execId,
            String[] blockIds,
            BlockFetchingListener listener,
            TransportConf transportConf,
            DownloadFileManager downloadFileManager) {
        this.client = client;
        this.blockIds = blockIds;
        this.listener = listener;
        this.chunkCallback = new ChunkCallback();
        this.transportConf = transportConf;
        this.downloadFileManager = downloadFileManager;
        if (blockIds.length == 0) {
            throw new IllegalArgumentException("Zero-sized blockIds array");
        }
        if (!transportConf.useOldFetchProtocol() && isShuffleBlocks(blockIds)) {
            this.message = createFetchShuffleBlocksMsg(appId, execId, blockIds);
        } else {
            this.message = new OpenBlocks(appId, execId, blockIds);
        }
    }

    private boolean isShuffleBlocks(String[] blockIds) {
        for (String blockId : blockIds) {
            if (!blockId.startsWith("shuffle_")) {
                return false;
            }
        }
        return true;
    }

    /**
     * Analyze the pass in blockIds and create FetchShuffleBlocks message.
     * The blockIds has been sorted by mapId and reduceId. It's produced in
     * org.apache.spark.MapOutputTracker.convertMapStatuses.
     */
    private FetchShuffleBlocks createFetchShuffleBlocksMsg(
            String appId, String execId, String[] blockIds) {
        int shuffleId = splitBlockId(blockIds[0]).left;
        HashMap<Long, ArrayList<Integer>> mapIdToReduceIds = new HashMap<>();
        for (String blockId : blockIds) {
            ImmutableTriple<Integer, Long, Integer> blockIdParts = splitBlockId(blockId);
            if (blockIdParts.left != shuffleId) {
                throw new IllegalArgumentException("Expected shuffleId=" + shuffleId +
                        ", got:" + blockId);
            }
            long mapId = blockIdParts.middle;
            if (!mapIdToReduceIds.containsKey(mapId)) {
                mapIdToReduceIds.put(mapId, new ArrayList<>());
            }
            mapIdToReduceIds.get(mapId).add(blockIdParts.right);
        }
        long[] mapIds = Longs.toArray(mapIdToReduceIds.keySet());
        int[][] reduceIdArr = new int[mapIds.length][];
        for (int i = 0; i < mapIds.length; i++) {
            reduceIdArr[i] = Ints.toArray(mapIdToReduceIds.get(mapIds[i]));
        }
        return new FetchShuffleBlocks(appId, execId, shuffleId, mapIds, reduceIdArr);
    }

    /**
     * Split the shuffleBlockId and return shuffleId, mapId and reduceId.
     */
    private ImmutableTriple<Integer, Long, Integer> splitBlockId(String blockId) {
        String[] blockIdParts = blockId.split("_");
        if (blockIdParts.length != 4 || !blockIdParts[0].equals("shuffle")) {
            throw new IllegalArgumentException(
                    "Unexpected shuffle block id format: " + blockId);
        }
        return new ImmutableTriple<>(
                Integer.parseInt(blockIdParts[1]),
                Long.parseLong(blockIdParts[2]),
                Integer.parseInt(blockIdParts[3]));
    }

    /**
     * Callback invoked on receipt of each chunk. We equate a single chunk to a single block.
     */
    private class ChunkCallback implements ChunkReceivedCallback {
        @Override
        public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
            // On receipt of a chunk, pass it upwards as a block.
            listener.onBlockFetchSuccess(blockIds[chunkIndex], buffer);
        }

        @Override
        public void onFailure(int chunkIndex, Throwable e) {
            // On receipt of a failure, fail every block from chunkIndex onwards.
            String[] remainingBlockIds = Arrays.copyOfRange(blockIds, chunkIndex, blockIds.length);
            failRemainingBlocks(remainingBlockIds, e);
        }
    }

    /**
     * Begins the fetching process, calling the listener with every block fetched.
     * The given message will be serialized with the Java serializer, and the RPC must return a
     * {@link StreamHandle}. We will send all fetch requests immediately, without throttling.
     */
    public void start() {

        //发送消息即 OpenBlocks消息
        client.sendRpc(message.toByteBuffer(), new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                try {

                    //成功后反序列化StreamHandle
                    streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
                    logger.trace("Successfully opened blocks {}, preparing to fetch chunks.", streamHandle);

                    // Immediately request all chunks -- we expect that the total size of the request is
                    // reasonable due to higher level chunking in [[ShuffleBlockFetcherIterator]].
                    //根据StreamHandle的numChunks由底到高遍历调用TransportClient的获取Block
                    for (int i = 0; i < streamHandle.numChunks; i++) {
                        if (downloadFileManager != null) {
                            client.stream(OneForOneStreamManager.genStreamChunkId(streamHandle.streamId, i),
                                    new DownloadCallback(i));
                        } else {
                            client.fetchChunk(streamHandle.streamId, i, chunkCallback);
                        }
                    }
                } catch (Exception e) {
                    logger.error("Failed while starting block fetches after success", e);
                    failRemainingBlocks(blockIds, e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                logger.error("Failed while starting block fetches", e);
                failRemainingBlocks(blockIds, e);
            }
        });
    }

    /**
     * Invokes the "onBlockFetchFailure" callback for every listed block id.
     */
    private void failRemainingBlocks(String[] failedBlockIds, Throwable e) {
        for (String blockId : failedBlockIds) {
            try {
                listener.onBlockFetchFailure(blockId, e);
            } catch (Exception e2) {
                logger.error("Error in block fetch failure callback", e2);
            }
        }
    }

    private class DownloadCallback implements StreamCallback {

        private DownloadFileWritableChannel channel = null;
        private DownloadFile targetFile = null;
        private int chunkIndex;

        DownloadCallback(int chunkIndex) throws IOException {
            this.targetFile = downloadFileManager.createTempFile(transportConf);
            this.channel = targetFile.openForWriting();
            this.chunkIndex = chunkIndex;
        }

        @Override
        public void onData(String streamId, ByteBuffer buf) throws IOException {
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
        }

        @Override
        public void onComplete(String streamId) throws IOException {
            listener.onBlockFetchSuccess(blockIds[chunkIndex], channel.closeAndRead());
            if (!downloadFileManager.registerTempFileToClean(targetFile)) {
                targetFile.delete();
            }
        }

        @Override
        public void onFailure(String streamId, Throwable cause) throws IOException {
            channel.close();
            // On receipt of a failure, fail every block from chunkIndex onwards.
            String[] remainingBlockIds = Arrays.copyOfRange(blockIds, chunkIndex, blockIds.length);
            failRemainingBlocks(remainingBlockIds, cause);
            targetFile.delete();
        }
    }
}
