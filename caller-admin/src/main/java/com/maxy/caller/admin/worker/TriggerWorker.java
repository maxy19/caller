package com.maxy.caller.admin.worker;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.maxy.caller.admin.cache.CacheService;
import com.maxy.caller.admin.service.AdminWorker;
import com.maxy.caller.bo.TaskDetailInfoBO;
import com.maxy.caller.common.utils.JSONUtils;
import com.maxy.caller.common.utils.timewheel.HashedWheelTimer;
import com.maxy.caller.core.config.GeneralConfigCenter;
import com.maxy.caller.core.config.ThreadPoolConfig;
import com.maxy.caller.core.config.ThreadPoolRegisterCenter;
import com.maxy.caller.core.enums.ExecutionStatusEnum;
import com.maxy.caller.core.netty.protocol.ProtocolMsg;
import com.maxy.caller.core.service.TaskBaseInfoService;
import com.maxy.caller.core.service.TaskDetailInfoService;
import com.maxy.caller.core.service.TaskLogService;
import com.maxy.caller.core.utils.CallerUtils;
import com.maxy.caller.dto.CallerTaskDTO;
import com.maxy.caller.pojo.Value;
import com.maxy.caller.remoting.server.netty.helper.NettyServerHelper;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.maxy.caller.admin.enums.RouterStrategyEnum.router;
import static com.maxy.caller.core.common.RpcHolder.REQUEST_MAP;
import static com.maxy.caller.core.constant.ThreadConstant.ADMIN_TRIGGER_BACKUP_WORKER_THREAD_POOL;
import static com.maxy.caller.core.constant.ThreadConstant.ADMIN_TRIGGER_WORKER_THREAD_POOL;
import static com.maxy.caller.core.constant.ThreadConstant.INVOKE_CLIENT_TASK_THREAD_POOL;
import static com.maxy.caller.core.constant.ThreadConstant.POP_LOOP_SLOT_THREAD_POOL;
import static com.maxy.caller.core.constant.ThreadConstant.RETRY_TASK_THREAD_POOL;
import static com.maxy.caller.core.enums.ExecutionStatusEnum.EXECUTION_FAILED;
import static com.maxy.caller.core.enums.ExecutionStatusEnum.EXECUTION_SUCCEED;
import static com.maxy.caller.core.enums.ExecutionStatusEnum.EXPIRED;
import static com.maxy.caller.core.enums.ExecutionStatusEnum.RETRYING;
import static com.maxy.caller.core.enums.GenerateKeyEnum.LIST_QUEUE_FORMAT_BACKUP;
import static com.maxy.caller.core.enums.GenerateKeyEnum.ZSET_QUEUE_FORMAT;
import static com.maxy.caller.core.utils.CallerUtils.parse;

/**
 * 从队列里面获取数据调用netty-rpc执行
 *
 * @Author maxy
 **/
@Component
@Log4j2
public class TriggerWorker implements AdminWorker {

    @Resource
    private CacheService cacheService;
    @Resource
    private TaskDetailInfoService taskDetailInfoService;
    @Resource
    private TaskBaseInfoService taskBaseInfoService;
    @Resource
    private TaskLogService taskLogService;
    @Resource
    private GeneralConfigCenter config;
    @Resource
    private NettyServerHelper nettyServerHelper;
    /**
     * 线程池
     */
    private ThreadPoolConfig threadPoolConfig = ThreadPoolConfig.getInstance();
    private ExecutorService worker = threadPoolConfig.getSingleThreadExecutor(true, ADMIN_TRIGGER_WORKER_THREAD_POOL);
    private ExecutorService backupWorker = threadPoolConfig.getSingleThreadExecutor(false, ADMIN_TRIGGER_BACKUP_WORKER_THREAD_POOL);
    private ExecutorService executorSchedule = threadPoolConfig.getPublicThreadPoolExecutor(false, INVOKE_CLIENT_TASK_THREAD_POOL);
    private ThreadPoolExecutor retryExecutor = threadPoolConfig.getPublicThreadPoolExecutor(true, RETRY_TASK_THREAD_POOL);
    private ThreadPoolExecutor loopSlotExecutor = threadPoolConfig.getPublicThreadPoolExecutor(true, POP_LOOP_SLOT_THREAD_POOL);

    {
        loopSlotExecutor.prestartAllCoreThreads();
        retryExecutor.prestartAllCoreThreads();
    }
    /**
     * 启动任务处理开关
     */
    private volatile boolean toggle = true;
    /**
     * 时间轮
     */
    private HashedWheelTimer delayTimer = new HashedWheelTimer(1, 256, 2 * Runtime.getRuntime().availableProcessors());
    private HashedWheelTimer timeOutTimer = new HashedWheelTimer(1, 1024, 2 * Runtime.getRuntime().availableProcessors());

    @PostConstruct
    public void init() {
        backupWorker.execute(() -> {
            for (int slot = 0, length = config.getTotalSlot(); slot < length; slot++) {
                List<String> keys = Lists.newArrayList(LIST_QUEUE_FORMAT_BACKUP.join(slot));
                List<String> tasks = cacheService.getQueueDataByBackup(keys, ImmutableList.of(config.getLimitNum()));
                if (CollectionUtils.isEmpty(tasks)) {
                    continue;
                }
                invoke(tasks);
            }
        });
    }

    @Override
    public void start() {
        worker.execute(() -> {
            while (toggle) {
                try {
                    pop();
                    //打散时间
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Exception e) {
                    log.error("队列获取数据出现异常", e);
                }
            }
        });
    }

    private void pop() {
        try {
            //获取索引列表
            for (int slot = 0, length = config.getTotalSlot(); slot <= length; slot++) {
                Value<Integer> indexValue = new Value<>(slot);
                loopSlotExecutor.execute(() -> {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    getAndInvokeAll(indexValue.getValue());
                    log.debug("pop#整个流程耗时:{} ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
                });
            }
        } catch (Exception e) {
            log.error("pop#执行出队时发现异常!!", e);
        }
    }

    /**
     * lua执行redis获取数据
     *
     * @param slot
     * @return
     */
    private void getAndInvokeAll(int slot) {
        int result;
        do {
            result = 0;
            List<String> keys = Lists.newArrayList(ZSET_QUEUE_FORMAT.join(slot), LIST_QUEUE_FORMAT_BACKUP.join(slot));
            long now = System.currentTimeMillis();
            //任务范围[1<now<10]
            String start = String.valueOf(now - ONE_MINUTE);
            String end = String.valueOf(now + TEN_MINUTE_OF_SECOND);
            List<String> args = Arrays.asList(start, end, "LIMIT", "0", config.getLimitNum());
            List<Object> queueData = cacheService.getQueueData(keys, args);
            if (CollectionUtils.isNotEmpty(queueData)) {
                log.info("找到数据!!,槽位:{}.", slot);
                invokeAll(queueData);
            }
            result += queueData.size();
        } while (result > 0);
    }

    /**
     * 检查过期任务
     *
     * @param context
     * @return
     */
    private boolean checkExpireTaskInfo(Pair<TaskDetailInfoBO, CallerTaskDTO> context) {
        TaskDetailInfoBO taskDetailInfoBO = context.getLeft();
        if (taskDetailInfoBO.getExecutionTime().getTime() <= (System.currentTimeMillis() - TWO_MINUTE)) {
            log.warn("checkExpireTaskInfo#[{}]任务,时间:[{}]已过期将丢弃.", getUniqueName(taskDetailInfoBO), taskDetailInfoBO.getExecutionTime());
            taskDetailInfoService.removeBackupCache(taskDetailInfoBO);
            //更新为过期
            if (taskDetailInfoService.updateStatus(taskDetailInfoBO.getId(), EXPIRED.getCode())) {
                taskLogService.save(taskDetailInfoBO);
            }
            return true;
        }
        return false;
    }

    /**
     * 执行时间轮
     *
     * @param queueData
     */
    private void invokeAll(List<Object> queueData) {
        for (int i = 0, length = queueData.size(); i < length; i++) {
            List<String> list = (List<String>) queueData.get(i);
            log.info("当前槽位数据量:{}", list.size());
            invoke(list);
        }
    }

    /**
     * 加入时间轮
     *
     * @param list
     */
    private void invoke(List<String> list) {
        for (String element : list) {
            TaskDetailInfoBO detailInfoBO = JSONUtils.parseObject(element, TaskDetailInfoBO.class);
            CallerTaskDTO dto = new CallerTaskDTO().fullField(detailInfoBO);
            Pair<TaskDetailInfoBO, CallerTaskDTO> context = Pair.of(detailInfoBO, dto);
            if (checkExpireTaskInfo(context)) {
                continue;
            }
            log.info("invoke#任务Id[{}]放入定时轮.", dto.getDetailTaskId());
            delayTimer.schedule(() -> {
                remoteClientMethod(context);
            }, dto.getExecutionTime().getTime() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }
    }


    /**
     * 通过netty调用远程方法
     *
     * @param context
     */
    private void remoteClientMethod(Pair<TaskDetailInfoBO, CallerTaskDTO> context) {
        //netty call client
        executorSchedule.execute(() -> {
            //1.移除已经不连接
            nettyServerHelper.removeNotActive(getGroupName(context.getLeft()));
            List<Channel> channels = nettyServerHelper.getActiveChannel().get(getGroupName(context.getLeft()));
            if (CollectionUtils.isEmpty(channels)) {
                log.error("remoteClientMethod#没有找到可以连接的通道!!!!.");
                return;
            }
            //2.获取channel
            Channel channel = getChannel(context.getRight(), channels);
            if (Objects.isNull(channel)) {
                log.error("remoteClientMethod#根据{}没有找到要发送的通道,请检查客户端是否存在!", context.getRight());
                return;
            }
            Value<Boolean> flag = new Value<>(true);
            //3.构建请求对象
            ProtocolMsg request = ProtocolMsg.toEntity(context.getRight());
            Stopwatch statistics = Stopwatch.createStarted();
            //4.发送并做超时检查
            sendAndCheckTimeOut(context, channel, flag, request);

            log.info("remoteClientMethod#请求Id：{},耗时:{}.", request.getRequestId(), statistics.elapsed(TimeUnit.MILLISECONDS));
        });
    }

    /**
     * 发送并检查超时
     *
     * @param context
     * @param channel
     * @param flag
     * @param request
     */
    private void sendAndCheckTimeOut(Pair<TaskDetailInfoBO, CallerTaskDTO> context,
                                     Channel channel,
                                     Value<Boolean> flag,
                                     ProtocolMsg request) {
        //key不存在添加
        REQUEST_MAP.putIfAbsent(request.getRequestId(), LOCK_DEFAULT);
        //发送
        send(channel, request, flag);
        if (BooleanUtils.isFalse(flag.getValue())) {
            log.warn("sendAndCheckTimeOut#当前通道属于非活跃状态,不再执行后续操作!");
            return;
        }
        //超时检查
        checkTimeOut(context, channel, request);
    }

    private void checkTimeOut(Pair<TaskDetailInfoBO, CallerTaskDTO> context, Channel channel, ProtocolMsg request) {
        //时间轮处理超时任务
        timeOutTimer.schedule(()-> {
            //key必须存在才能修改value值,如果已超时，先加个乐观锁,然后处理.
            Preconditions.checkArgument(request.getRequestId() != null);
            Value<Boolean> flag = new Value<>(true);
            Byte newVal = REQUEST_MAP.computeIfPresent(request.getRequestId(), (k, v) -> v == LOCK_DEFAULT ? LOCK_FAIL : v);
            //抢锁成功设置超时标记
            if (Objects.equals(newVal, LOCK_FAIL)) {
                //失败并触发重试机制
                if (context.getRight().getRetryNum() > 0) {
                    retry(context, channel, flag, request);
                } else {
                    recordResultAndCleanCache(context, channel, (byte) 0, EXECUTION_FAILED);
                }
            }
        }, context.getRight().getTimeout(), TimeUnit.MILLISECONDS);
    }

    /**
     * 获取channel
     *
     * @param callerTaskDTO
     * @param channels
     * @return
     */
    private Channel getChannel(CallerTaskDTO callerTaskDTO, List<Channel> channels) {
        return nettyServerHelper.getChannelByAddr(
                router(taskBaseInfoService.getRouterStrategy(callerTaskDTO.getGroupKey(),
                        callerTaskDTO.getBizKey(),
                        callerTaskDTO.getTopic()),
                        parse(channels)));
    }

    /**
     * 重试
     */
    private void retry(Pair<TaskDetailInfoBO, CallerTaskDTO> context, Channel channel, Value<Boolean> flag, ProtocolMsg request) {
        //更新detail为重试中
        context.getLeft().setExecutionStatus(RETRYING.getCode());
        taskDetailInfoService.update(context.getLeft());
        for (byte retryNum = 1, totalNum = context.getLeft().getRetryNum(); retryNum <= totalNum; retryNum++) {
            sendAndCheckTimeOut(context, channel, flag, request);
            taskLogService.save(context.getLeft(), parse(channel), retryNum);
            //检查是否成功
            if (retryNum == totalNum && BooleanUtils.isFalse(flag.getValue())) {
                log.info("retry#超时任务Id:{},已重试第{}次.结果:失败!!", context.getLeft().getId(),retryNum);
                recordResultAndCleanCache(context, channel, retryNum, EXECUTION_FAILED);
                REQUEST_MAP.remove(request.getRequestId());
                return;
            } else if (BooleanUtils.isTrue(flag.getValue())) {
                log.info("retry#超时任务Id:{},重试第{}次.结果:成功!!", context.getLeft().getId(),retryNum);
                recordResultAndCleanCache(context, channel, retryNum, EXECUTION_SUCCEED);
                REQUEST_MAP.remove(request.getRequestId());
                return;
            }
        }
    }

    private void recordResultAndCleanCache(Pair<TaskDetailInfoBO, CallerTaskDTO> context,
                                           Channel channel,
                                           byte retryNum,
                                           ExecutionStatusEnum statusEnum) {
        taskDetailInfoService.removeBackupCache(context.getLeft());
        context.getLeft().setExecutionStatus(statusEnum.getCode());
        taskDetailInfoService.update(context.getLeft());
        taskLogService.save(context.getLeft(), parse(channel), retryNum);
    }


    /**
     * 写入并发送
     *
     * @param channel
     * @param request
     * @param flag
     * @return
     */
    private void send(Channel channel, ProtocolMsg request, Value<Boolean> flag) {
        ChannelFuture channelFuture = null;
        if (!CallerUtils.isChannelActive(channel)) {
            flag.setValue(false);
        }
        channelFuture = channel.writeAndFlush(request);
        flag.setValue(CallerUtils.monitor(channelFuture));
    }


    @Override
    public void stop() {
        toggle = false;
        ThreadPoolRegisterCenter.destroy(backupWorker, retryExecutor, executorSchedule, worker);
    }

}
