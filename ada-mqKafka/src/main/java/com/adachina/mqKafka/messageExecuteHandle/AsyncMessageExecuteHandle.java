package com.adachina.mqKafka.messageExecuteHandle;

import com.adachina.mqKafka.handlers.MessageHandler;
import com.adachina.mqKafka.mainThreadEnum.MainThreadStatusEnum;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.*;

/**
 * @ProjectName: kclient
 * @Package: com.robert.kafka.kclient.messageExecuteHandle
 * @ClassName: AsyncMessageExecuteHandle
 * @Author: litianlong
 * @Description: ${description}
 * @Date: 2019-03-15 15:39
 * @Version: 1.0
 */
public class AsyncMessageExecuteHandle extends AbstractMessageExecuteHandle  {



    private ConcurrentLinkedQueue<ConsumerRecords<String, String>> concurrentLinkedQueue = new ConcurrentLinkedQueue();

    private ExecutorService streamThreadPool;

    private ExecutorService sharedAsyncThreadPool;

    private List<AbstractMessageTask> tasks;

    //

    private int streamNum = 1;

    private boolean isAutoCommitOffset = true;



    private int fixedThreadNum = 0;

    private int minThreadNum = 0;
    private int maxThreadNum = 0;

    private boolean isAsyncThreadModel = false;

    private boolean isSharedAsyncThreadPool = false;

    public AsyncMessageExecuteHandle(String topic,MessageHandler handler) {
        super(topic,handler);

    }


    public AsyncMessageExecuteHandle(String topic, MessageHandler handler, int streamNum) {
        this(topic, handler, streamNum, 0, false);
    }

    public AsyncMessageExecuteHandle(String topic, MessageHandler handler, int streamNum, int fixedThreadNum) {
        this(topic, handler, streamNum, fixedThreadNum, false);
    }

    public AsyncMessageExecuteHandle(String topic, MessageHandler handler, int streamNum, int fixedThreadNum, boolean isAsyncThreadModel) {
        super(topic, handler);
        this.streamNum = streamNum;
        this.fixedThreadNum = fixedThreadNum;
        this.isAsyncThreadModel = isAsyncThreadModel;
        this.isSharedAsyncThreadPool = (fixedThreadNum != 0);
    }


    public AsyncMessageExecuteHandle(String topic, MessageHandler handler, int streamNum, int minThreadNum, int maxThreadNum) {
        this(topic, handler, streamNum, minThreadNum,maxThreadNum, false);
    }

    public AsyncMessageExecuteHandle(String topic, MessageHandler handler, int streamNum, int minThreadNum, int maxThreadNum, boolean isSharedAsyncThreadPool) {
        super(topic, handler);
        this.streamNum = streamNum;
        this.minThreadNum = minThreadNum;
        this.maxThreadNum = maxThreadNum;
        this.isAsyncThreadModel =  !(minThreadNum == 0 && maxThreadNum == 0);
        this.isSharedAsyncThreadPool = isSharedAsyncThreadPool;
    }

    @Override
    public void init() {

        super.init();

        if (isAsyncThreadModel == true && fixedThreadNum <= 0
                && (minThreadNum <= 0 || maxThreadNum <= 0)) {
            log.error("Either fixedThreadNum or minThreadNum/maxThreadNum is greater than 0.");
            throw new IllegalArgumentException(
                    "Either fixedThreadNum or minThreadNum/maxThreadNum is greater than 0.");
        }

        if (isAsyncThreadModel == true && minThreadNum > maxThreadNum) {
            log.error("The minThreadNum should be less than maxThreadNum.");
            throw new IllegalArgumentException(
                    "The minThreadNum should be less than maxThreadNum.");
        }

        if (isSharedAsyncThreadPool) {
            sharedAsyncThreadPool = initAsyncThreadPool();
        }

    }

    @Override
    public void execute() {

        // 消费者订阅的topic, 可同时订阅多个
        consumer.subscribe(Arrays.asList(topic));

        String timeOut = properties.getProperty("ada.test.timeOut");

        Long timeOutLong = StringUtils.isEmpty(timeOut) ? defTimeOutLong : Long.parseLong(timeOut);

        tasks = new ArrayList<AbstractMessageTask>();

        for (int i = 0; i < streamNum ; i++) {

            AbstractMessageTask abstractMessageTask = (fixedThreadNum == 0 ? new SequentialMessageTask(handler) : new ConcurrentMessageTask(handler, fixedThreadNum));

            tasks.add(abstractMessageTask);
            streamThreadPool.execute(abstractMessageTask);
        }

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(timeOutLong);

                if (!records.isEmpty()) {
                    concurrentLinkedQueue.add(records);
                }

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }

                consumer.commitAsync();
            }
        } catch (Exception e) {

            log.error(e.getMessage());
        }
        finally {

            try{
                consumer.commitSync();
            } finally {
                consumer.close();
            }

        }
    }

    @Override
    public void initKafka(Properties properties) {

        super.initKafka(properties);

        streamThreadPool = Executors.newFixedThreadPool(streamNum);

    }

    public abstract class AbstractMessageTask implements Runnable {



        protected MessageHandler messageHandler;

        AbstractMessageTask(MessageHandler messageHandler) {
            this.messageHandler = messageHandler;
        }

        @Override
        public void run() {

            System.out.println("线程："+Thread.currentThread().getName()+"启动！");

            while (status == MainThreadStatusEnum.RUNNING) {

                ConsumerRecords<String, String> consumerRecords = concurrentLinkedQueue.poll();

                if (consumerRecords == null) {

                    continue;
                }
                System.out.println("线程："+Thread.currentThread().getName()+"处理任务："+consumerRecords.toString());


                Iterator<ConsumerRecord<String, String>> it = consumerRecords.iterator();

                boolean hasNext = false;
                try {
                    // When it is interrupted if process is killed, it causes some duplicate message processing, because it commits the message in a chunk every 30 seconds
                    hasNext = it.hasNext();
                } catch (Exception e) {
                    // hasNext() method is implemented by scala, so no checked
                    // exception is declared, in addtion, hasNext() may throw
                    // Interrupted exception when interrupted, so we have to
                    // catch Exception here and then decide if it is interrupted
                    // exception
                    if (e instanceof InterruptedException) {
                        log.info(
                                "The worker [Thread ID: {}] has been interrupted when retrieving messages from kafka broker. Maybe the consumer is shutting down.",
                                Thread.currentThread().getId());
                        log.error("Retrieve Interrupted: ", e);

                        if (status != MainThreadStatusEnum.RUNNING) {
//							it.clearCurrentChunk();
                            shutdown();
                            break;
                        }
                    } else {
                        log.error(
                                "The worker [Thread ID: {}] encounters an unknown exception when retrieving messages from kafka broker. Now try again.",
                                Thread.currentThread().getId());
                        log.error("Retrieve Error: ", e);
                        continue;
                    }
                }

                while (hasNext) {

                    ConsumerRecord<String, String> item = it.next();
                    log.info("partition[" + item.partition() + "] offset[" + item.offset() + "] message[" + item.value() + "]");

                    handleMessage(item.value());

                    // if not auto commit, commit it manually
                    if (!isAutoCommitOffset) {
                    }

                    hasNext = it.hasNext();

                }

            }


        }

        protected void shutdown() {

            // Actually it doesn't work in auto commit mode, because kafka v0.8 commits once per 30 seconds, so it is bound to consume duplicate messages.

        }

        protected abstract void handleMessage(String message);
    }

    public class SequentialMessageTask extends AbstractMessageTask {
        public SequentialMessageTask(MessageHandler messageHandler) {
            super( messageHandler);
        }

        @Override
        protected void handleMessage(String message) {

            messageHandler.execute(message);
        }
    }

    public class ConcurrentMessageTask extends AbstractMessageTask {
        private ExecutorService asyncThreadPool;

        public ConcurrentMessageTask(MessageHandler messageHandler, int threadNum) {
            super( messageHandler);

            if (isSharedAsyncThreadPool) {
                asyncThreadPool = sharedAsyncThreadPool;
            } else {
                asyncThreadPool = initAsyncThreadPool();
            }
        }

        @Override
        protected void handleMessage(final String message) {
            asyncThreadPool.submit(new Runnable() {
                @Override
                public void run() {
                    // if it blows, how to recover
                    messageHandler.execute(message);
                }
            });
        }

        @Override
        protected void shutdown() {
            if (!isSharedAsyncThreadPool) {
                shutdownThreadPool(asyncThreadPool, "async-pool-" + Thread.currentThread().getId());
            }
        }
    }

    @Override
    public void shutdownGracefully() {

        System.out.println("AsyncMessageExecuteHandle+shutdownGracefully+1");

        status = MainThreadStatusEnum.STOPPING;

        shutdownThreadPool(streamThreadPool, "main-pool");

        if (isSharedAsyncThreadPool) {
            shutdownThreadPool(sharedAsyncThreadPool, "shared-async-pool");
        }
        else {
            for (AsyncMessageExecuteHandle.AbstractMessageTask task : tasks) {

                System.out.println("AsyncMessageExecuteHandle+shutdownGracefully+tasks+1");
                task.shutdown();
            }
        }


        status = MainThreadStatusEnum.STOPPED;
        System.out.println("AsyncMessageExecuteHandle+shutdownGracefully+2");
    }

    private void shutdownThreadPool(ExecutorService threadPool, String alias) {
        log.info("Start to shutdown the thead pool: {}", alias);

        threadPool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                threadPool.shutdownNow(); // Cancel currently executing tasks
                log.warn("Interrupt the worker, which may cause some task inconsistent. Please check the biz logs.");

                // Wait a while for tasks to respond to being cancelled
                if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                    log.error("Thread pool can't be shutdown even with interrupting worker threads, which may cause some task inconsistent. Please check the biz logs.");
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            threadPool.shutdownNow();
            log.error("The current server thread is interrupted when it is trying to stop the worker threads. This may leave an inconcistent state. Please check the biz logs.");

            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }

        log.info("Finally shutdown the thead pool: {}", alias);
    }

    public ExecutorService initAsyncThreadPool() {
        ExecutorService syncThreadPool = null;
        if (fixedThreadNum > 0) {
            syncThreadPool = Executors.newFixedThreadPool(fixedThreadNum);
        }else {
            syncThreadPool = new ThreadPoolExecutor(minThreadNum, maxThreadNum, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
        }

        return syncThreadPool;
    }
}
