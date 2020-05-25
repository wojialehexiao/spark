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

package org.apache.spark.memory;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.*;

/**
 * Manages the memory allocated by an individual task.
 * <p>
 * Most of the complexity in this class deals with encoding of off-heap addresses into 64-bit longs.
 * In off-heap mode, memory can be directly addressed with 64-bit longs. In on-heap mode, memory is
 * addressed by the combination of a base Object reference and a 64-bit offset within that object.
 * This is a problem when we want to store pointers to data structures inside of other structures,
 * such as record pointers inside hashmaps or sorting buffers. Even if we decided to use 128 bits
 * to address memory, we can't just store the address of the base object since it's not guaranteed
 * to remain stable as the heap gets reorganized due to GC.
 * <p>
 * Instead, we use the following approach to encode record pointers in 64-bit longs: for off-heap
 * mode, just store the raw address, and for on-heap mode use the upper 13 bits of the address to
 * store a "page number" and the lower 51 bits to store an offset within this page. These page
 * numbers are used to index into a "page table" array inside of the MemoryManager in order to
 * retrieve the base object.
 * <p>
 * This allows us to address 8192 pages. In on-heap mode, the maximum page size is limited by the
 * maximum size of a long[] array, allowing us to address 8192 * (2^31 - 1) * 8 bytes, which is
 * approximately 140 terabytes of memory.
 */
public class TaskMemoryManager {

    private static final Logger logger = LoggerFactory.getLogger(TaskMemoryManager.class);

    /**
     * The number of bits used to address the page table.
     * 用于寻址Page表的位数。 在64位的长整型中将使用高位的13位存储页号
     */
    private static final int PAGE_NUMBER_BITS = 13;

    /**
     * The number of bits used to encode offsets in data pages.
     * 用于保存编码后的偏移量的位数。
     * 在64位的长整型中使用地位的51位存储偏移量
     */
    @VisibleForTesting
    static final int OFFSET_BITS = 64 - PAGE_NUMBER_BITS;  // 51

    /**
     * The number of entries in the page table.
     * Page表中的Page数量
     */
    private static final int PAGE_TABLE_SIZE = 1 << PAGE_NUMBER_BITS;

    /**
     * Maximum supported data page size (in bytes). In principle, the maximum addressable page size is
     * (1L &lt;&lt; OFFSET_BITS) bytes, which is 2+ petabytes. However, the on-heap allocator's
     * maximum page size is limited by the maximum amount of data that can be stored in a long[]
     * array, which is (2^31 - 1) * 8 bytes (or about 17 gigabytes). Therefore, we cap this at 17
     * gigabytes.
     * <p>
     * 最大的Page大小。
     */
    public static final long MAXIMUM_PAGE_SIZE_BYTES = ((1L << 31) - 1) * 8L;

    /**
     * Bit mask for the lower 51 bits of a long.
     * 长整型的低51位的位掩码。
     */
    private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;

    /**
     * Similar to an operating system's page table, this array maps page numbers into base object
     * pointers, allowing us to translate between the hashtable's internal 64-bit address
     * representation and the baseObject+offset representation which we use to support both on- and
     * off-heap addresses. When using an off-heap allocator, every entry in this map will be `null`.
     * When using an on-heap allocator, the entries in this map will point to pages' base objects.
     * Entries are added to this map as new data pages are allocated.
     * <p>
     * Page表。
     */
    private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];

    /**
     * Bitmap for tracking free pages.
     * <p>
     * 用于跟踪空闲Page的BitSet
     */
    private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);

    private final MemoryManager memoryManager;

    /**
     * TaskMemoryManager所管理任务尝试的身份标识
     */
    private final long taskAttemptId;

    /**
     * Tracks whether we're on-heap or off-heap. For off-heap, we short-circuit most of these methods
     * without doing any masking or lookups. Since this branching should be well-predicted by the JIT,
     * this extra layer of indirection / abstraction hopefully shouldn't be too expensive.
     * <p>
     * 内存模式
     */
    final MemoryMode tungstenMemoryMode;

    /**
     * Tracks spillable memory consumers.
     * <p>
     * 用于跟踪可溢出的内存消费者
     */
    @GuardedBy("this")
    private final HashSet<MemoryConsumer> consumers;

    /**
     * The amount of memory that is acquired but not used.
     * <p>
     * 任务尝试已经获得但是并未使用的内存大小
     */
    private volatile long acquiredButNotUsed = 0L;

    /**
     * Construct a new TaskMemoryManager.
     */
    public TaskMemoryManager(MemoryManager memoryManager, long taskAttemptId) {
        this.tungstenMemoryMode = memoryManager.tungstenMemoryMode();
        this.memoryManager = memoryManager;
        this.taskAttemptId = taskAttemptId;
        this.consumers = new HashSet<>();
    }

    /**
     * Acquire N bytes of memory for a consumer. If there is no enough memory, it will call
     * spill() of consumers to release more memory.
     * <p>
     * 为内存消费者获得指定大小的内存。
     * 当Task没有足够的内存是， 将调用MemoryConsumer的spill方法释放内存
     *
     * @return number of bytes successfully granted (<= N).
     */
    public long acquireExecutionMemory(long required, MemoryConsumer consumer) {
        assert (required >= 0);
        assert (consumer != null);
        MemoryMode mode = consumer.getMode();
        // If we are allocating Tungsten pages off-heap and receive a request to allocate on-heap
        // memory here, then it may not make sense to spill since that would only end up freeing
        // off-heap memory. This is subject to change, though, so it may be risky to make this
        // optimization now in case we forget to undo it late when making changes.
        synchronized (this) {

            //为当前的任务尝试按照指定的存储模式获取指定大小的内存
            long got = memoryManager.acquireExecutionMemory(required, taskAttemptId, mode);

            // Try to release memory from other consumers first,
            // then we can reduce the frequency of spilling, avoid to have too many spilled files.
            // 尝试从其他consumer释放内存
            // 然后我们可以减少溢出的频率，避免文件溢出过多。
            if (got < required) {


                // Call spill() on other consumers to release memory
                // Sort the consumers according their memory usage. So we avoid spilling the same consumer
                // which is just spilled in last few times and re-spilling on it will produce many small
                // spill files.
                TreeMap<Long, List<MemoryConsumer>> sortedConsumers = new TreeMap<>();
                for (MemoryConsumer c : consumers) {
                    if (c != consumer && c.getUsed() > 0 && c.getMode() == mode) {
                        long key = c.getUsed();
                        List<MemoryConsumer> list =
                                sortedConsumers.computeIfAbsent(key, k -> new ArrayList<>(1));
                        list.add(c);
                    }
                }


                while (!sortedConsumers.isEmpty()) {
                    // Get the consumer using the least memory more than the remaining required memory.
                    Map.Entry<Long, List<MemoryConsumer>> currentEntry =
                            sortedConsumers.ceilingEntry(required - got);
                    // No consumer has used memory more than the remaining required memory.
                    // Get the consumer of largest used memory.
                    if (currentEntry == null) {
                        currentEntry = sortedConsumers.lastEntry();
                    }
                    List<MemoryConsumer> cList = currentEntry.getValue();
                    MemoryConsumer c = cList.get(cList.size() - 1);
                    try {

                        //从其他Consumer释放内存，溢出数据到磁盘
                        long released = c.spill(required - got, consumer);
                        if (released > 0) {
                            logger.debug("Task {} released {} from {} for {}", taskAttemptId,
                                    Utils.bytesToString(released), c, consumer);


                            got += memoryManager.acquireExecutionMemory(required - got, taskAttemptId, mode);
                            if (got >= required) {
                                break;
                            }
                        } else {
                            cList.remove(cList.size() - 1);
                            if (cList.isEmpty()) {
                                sortedConsumers.remove(currentEntry.getKey());
                            }
                        }
                    } catch (ClosedByInterruptException e) {
                        // This called by user to kill a task (e.g: speculative task).
                        logger.error("error while calling spill() on " + c, e);
                        throw new RuntimeException(e.getMessage());
                    } catch (IOException e) {
                        logger.error("error while calling spill() on " + c, e);
                        // checkstyle.off: RegexpSinglelineJava
                        throw new SparkOutOfMemoryError("error while calling spill() on " + c + " : "
                                + e.getMessage());
                        // checkstyle.on: RegexpSinglelineJava
                    }
                }
            }

            // call spill() on itself
            if (got < required) {
                try {

                    //为当前任务尝试腾出内存
                    long released = consumer.spill(required - got, consumer);
                    if (released > 0) {
                        logger.debug("Task {} released {} from itself ({})", taskAttemptId,
                                Utils.bytesToString(released), consumer);
                        got += memoryManager.acquireExecutionMemory(required - got, taskAttemptId, mode);
                    }

                } catch (ClosedByInterruptException e) {
                    // This called by user to kill a task (e.g: speculative task).
                    logger.error("error while calling spill() on " + consumer, e);
                    throw new RuntimeException(e.getMessage());
                } catch (IOException e) {
                    logger.error("error while calling spill() on " + consumer, e);
                    // checkstyle.off: RegexpSinglelineJava
                    throw new SparkOutOfMemoryError("error while calling spill() on " + consumer + " : "
                            + e.getMessage());
                    // checkstyle.on: RegexpSinglelineJava
                }
            }

            //将当前申请获得内存的MemoryConsumer添加到consumer中
            consumers.add(consumer);
            logger.debug("Task {} acquired {} for {}", taskAttemptId, Utils.bytesToString(got), consumer);

            //返回最终获得的内存大小
            return got;
        }
    }

    /**
     * Release N bytes of execution memory for a MemoryConsumer.
     * 为内存消费者释放指定大小的内存
     */
    public void releaseExecutionMemory(long size, MemoryConsumer consumer) {
        logger.debug("Task {} release {} from {}", taskAttemptId, Utils.bytesToString(size), consumer);
        memoryManager.releaseExecutionMemory(size, taskAttemptId, consumer.getMode());
    }

    /**
     * Dump the memory usage of all consumers.
     * 用于将任务尝试各个MemoryConsumer及MemoryManager管理的执行内存和存储内存的实际情况打印到日志
     */
    public void showMemoryUsage() {
        logger.info("Memory used in task " + taskAttemptId);
        synchronized (this) {
            long memoryAccountedForByConsumers = 0;
            for (MemoryConsumer c : consumers) {
                long totalMemUsage = c.getUsed();
                memoryAccountedForByConsumers += totalMemUsage;
                if (totalMemUsage > 0) {
                    logger.info("Acquired by " + c + ": " + Utils.bytesToString(totalMemUsage));
                }
            }
            long memoryNotAccountedFor =
                    memoryManager.getExecutionMemoryUsageForTask(taskAttemptId) - memoryAccountedForByConsumers;
            logger.info(
                    "{} bytes of memory were used by task {} but are not associated with specific consumers",
                    memoryNotAccountedFor, taskAttemptId);
            logger.info(
                    "{} bytes of memory are used for execution and {} bytes of memory are used for storage",
                    memoryManager.executionMemoryUsed(), memoryManager.storageMemoryUsed());
        }
    }

    /**
     * Return the page size in bytes.
     *
     * 用于获得Page的大小。
     */
    public long pageSizeBytes() {
        return memoryManager.pageSizeBytes();
    }

    /**
     * Allocate a block of memory that will be tracked in the MemoryManager's page table; this is
     * intended for allocating large blocks of Tungsten memory that will be shared between operators.
     * <p>
     * Returns `null` if there was not enough memory to allocate the page. May return a page that
     * contains fewer bytes than requested, so callers should verify the size of returned pages.
     *
     * 用于给指定MemoryConsumer分配指定大小的MemoryBlock
     * @throws TooLargePageException
     */
    public MemoryBlock allocatePage(long size, MemoryConsumer consumer) {
        assert (consumer != null);
        assert (consumer.getMode() == tungstenMemoryMode);

        //获得的页大小不能超出限制
        if (size > MAXIMUM_PAGE_SIZE_BYTES) {
            throw new TooLargePageException(size);
        }

        // 获取逻辑内存
        long acquired = acquireExecutionMemory(size, consumer);
        if (acquired <= 0) {
            return null;
        }

        final int pageNumber;
        synchronized (this) {
            //获得还未分配的页号
            pageNumber = allocatedPages.nextClearBit(0);
            if (pageNumber >= PAGE_TABLE_SIZE) {
                releaseExecutionMemory(acquired, consumer);
                throw new IllegalStateException(
                        "Have already allocated a maximum of " + PAGE_TABLE_SIZE + " pages");
            }

            //将此页号记为已分配
            allocatedPages.set(pageNumber);
        }


        MemoryBlock page = null;
        try {

            // 分配指定大小的MemoryBlock
            page = memoryManager.tungstenMemoryAllocator().allocate(acquired);

        } catch (OutOfMemoryError e) {
            logger.warn("Failed to allocate a page ({} bytes), try again.", acquired);
            // there is no enough memory actually, it means the actual free memory is smaller than
            // MemoryManager thought, we should keep the acquired memory.
            synchronized (this) {
                acquiredButNotUsed += acquired;
                allocatedPages.clear(pageNumber);
            }
            // this could trigger spilling to free some pages.
            return allocatePage(size, consumer);
        }

        page.pageNumber = pageNumber;

        //将页号对应也放入页表
        pageTable[pageNumber] = page;
        if (logger.isTraceEnabled()) {
            logger.trace("Allocate page number {} ({} bytes)", pageNumber, acquired);
        }

        return page;
    }

    /**
     * Free a block of memory allocated via {@link TaskMemoryManager#allocatePage}.
     *
     * 释放给MemoryConsumer分配的MemoryBlock
     */
    public void freePage(MemoryBlock page, MemoryConsumer consumer) {
        assert (page.pageNumber != MemoryBlock.NO_PAGE_NUMBER) :
                "Called freePage() on memory that wasn't allocated with allocatePage()";
        assert (page.pageNumber != MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER) :
                "Called freePage() on a memory block that has already been freed";
        assert (page.pageNumber != MemoryBlock.FREED_IN_TMM_PAGE_NUMBER) :
                "Called freePage() on a memory block that has already been freed";
        assert (allocatedPages.get(page.pageNumber));

        //清理pageTable指定页号对应的MemoryBlock
        pageTable[page.pageNumber] = null;

        synchronized (this) {
            //清空allocate的Page对MemoryBlock的页号跟踪
            allocatedPages.clear(page.pageNumber);
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Freed page number {} ({} bytes)", page.pageNumber, page.size());
        }

        long pageSize = page.size();
        // Clear the page number before passing the block to the MemoryAllocator's free().
        // Doing this allows the MemoryAllocator to detect when a TaskMemoryManager-managed
        // page has been inappropriately directly freed without calling TMM.freePage().
        page.pageNumber = MemoryBlock.FREED_IN_TMM_PAGE_NUMBER;

        // 释放MemoryBlock
        memoryManager.tungstenMemoryAllocator().free(page);

        //释放MemoryManager管理的逻辑内存
        releaseExecutionMemory(pageSize, consumer);
    }

    /**
     * Given a memory page and offset within that page, encode this address into a 64-bit long.
     * This address will remain valid as long as the corresponding page has not been freed.
     *
     * 用于跟踪给定的Page（MemoryBlock）和Page中偏移量的地址， 返回页号和相对于内存其实地址的偏移量
     *
     * @param page         a data page allocated by {@link TaskMemoryManager#allocatePage}/
     * @param offsetInPage an offset in this page which incorporates the base offset. In other words,
     *                     this should be the value that you would pass as the base offset into an
     *                     UNSAFE call (e.g. page.baseOffset() + something).
     * @return an encoded page address.
     */
    public long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage) {

        //如果内存模式是堆外内存，此时的参数offsetInPage是操作系统内存的绝对地址
        //offsetInPage与MemoryBlock的起始地址之差就是相对于起始地址的偏移量（64位地址的低51位）
        if (tungstenMemoryMode == MemoryMode.OFF_HEAP) {
            // In off-heap mode, an offset is an absolute address that may require a full 64 bits to
            // encode. Due to our page size limitation, though, we can convert this into an offset that's
            // relative to the page's base offset; this relative offset will fit in 51 bits.
            // 获取页的偏移量
            offsetInPage -= page.getBaseOffset();
        }
        return encodePageNumberAndOffset(page.pageNumber, offsetInPage);
    }


    /**
     * 获取页号相对于内存起始地址的偏移量
     * 通过位运算将页号存储到64位长整型的高13位中，并将偏移量存储到64位长整型中的低51位中，返回生成的64位长整型
     * @param pageNumber
     * @param offsetInPage
     * @return
     */
    @VisibleForTesting
    public static long encodePageNumberAndOffset(int pageNumber, long offsetInPage) {
        assert (pageNumber >= 0) : "encodePageNumberAndOffset called with invalid page";
        return (((long) pageNumber) << OFFSET_BITS) | (offsetInPage & MASK_LONG_LOWER_51_BITS);
    }


    /**
     * 用于将64位的长整型右移51位（只剩下页号），然后转换为整型以获得Page的页号
     * @param pagePlusOffsetAddress
     * @return
     */
    @VisibleForTesting
    public static int decodePageNumber(long pagePlusOffsetAddress) {
        return (int) (pagePlusOffsetAddress >>> OFFSET_BITS);
    }

    /**
     * 用于将64位的长整型与51位的掩码按位进行与运算， 以获得在Page中的偏移量
     * @param pagePlusOffsetAddress
     * @return
     */
    private static long decodeOffset(long pagePlusOffsetAddress) {
        return (pagePlusOffsetAddress & MASK_LONG_LOWER_51_BITS);
    }

    /**
     * Get the page associated with an address encoded by
     * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
     *
     * 用于通过64位的长整型，获取Page在内存中的对象。
     * 此方法度体育采用堆内存模式有效，否则返回null
     */
    public Object getPage(long pagePlusOffsetAddress) {
        if (tungstenMemoryMode == MemoryMode.ON_HEAP) {
            final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
            assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
            final MemoryBlock page = pageTable[pageNumber];
            assert (page != null);
            assert (page.getBaseObject() != null);
            return page.getBaseObject();
        } else {
            return null;
        }
    }

    /**
     * Get the offset associated with an address encoded by
     * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
     *
     * 用于通过64位长整型，获取在Page中的偏移量
     */
    public long getOffsetInPage(long pagePlusOffsetAddress) {

        //获得在Page中的偏移量
        final long offsetInPage = decodeOffset(pagePlusOffsetAddress);


        if (tungstenMemoryMode == MemoryMode.ON_HEAP) {
            //如果是堆内存模式， 返回Page中的偏移量
            return offsetInPage;
        } else {
            // In off-heap mode, an offset is an absolute address. In encodePageNumberAndOffset, we
            // converted the absolute address into a relative address. Here, we invert that operation:
            // 获得Page的页号
            final int pageNumber = decodePageNumber(pagePlusOffsetAddress);

            assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);

            // 从PageTable中获得与页号对应的MemoryBlock
            final MemoryBlock page = pageTable[pageNumber];
            assert (page != null);

            //返回Page在操系统中的偏移量
            return page.getBaseOffset() + offsetInPage;
        }
    }

    /**
     * Clean up all allocated memory and pages. Returns the number of bytes freed. A non-zero return
     * value can be used to detect memory leaks.
     *
     * 清空所有Page和内存的方法
     */
    public long cleanUpAllAllocatedMemory() {
        synchronized (this) {
            for (MemoryConsumer c : consumers) {
                if (c != null && c.getUsed() > 0) {
                    // In case of failed task, it's normal to see leaked memory
                    logger.debug("unreleased " + Utils.bytesToString(c.getUsed()) + " memory from " + c);
                }
            }
            consumers.clear();

            for (MemoryBlock page : pageTable) {
                if (page != null) {
                    logger.debug("unreleased page: " + page + " in task " + taskAttemptId);
                    page.pageNumber = MemoryBlock.FREED_IN_TMM_PAGE_NUMBER;
                    memoryManager.tungstenMemoryAllocator().free(page);
                }
            }
            Arrays.fill(pageTable, null);
        }

        // release the memory that is not used by any consumer (acquired for pages in tungsten mode).
        memoryManager.releaseExecutionMemory(acquiredButNotUsed, taskAttemptId, tungstenMemoryMode);

        return memoryManager.releaseAllExecutionMemoryForTask(taskAttemptId);
    }

    /**
     * Returns the memory consumption, in bytes, for the current task.
     *
     * 用于获取任务尝试消费的所有内存大小
     */
    public long getMemoryConsumptionForThisTask() {
        return memoryManager.getExecutionMemoryUsageForTask(taskAttemptId);
    }

    /**
     * Returns Tungsten memory mode
     *
     */
    public MemoryMode getTungstenMemoryMode() {
        return tungstenMemoryMode;
    }
}
