package com.maxy.caller.admin.test;

import com.maxy.caller.bo.TaskDetailInfoBO;
import com.maxy.caller.common.utils.LocalDateUtils;
import com.maxy.caller.core.service.TaskDetailInfoService;
import lombok.SneakyThrows;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author maxuyang
 **/
@RunWith(SpringRunner.class)
@SpringBootTest
public class TaskDetailInfoTest {
    @Resource
    private TaskDetailInfoService taskDetailInfoService;

    @SneakyThrows
    @Test
    public void batchInsert() {
        List<TaskDetailInfoBO> list = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            TaskDetailInfoBO taskDetailInfoBO = new TaskDetailInfoBO();
            taskDetailInfoBO.setGroupKey("taobao");
            taskDetailInfoBO.setBizKey("order");
            taskDetailInfoBO.setTopic("clsExpireOrder");
            taskDetailInfoBO.setExecutionParam("test"+i);
            taskDetailInfoBO.setExecutionTime(LocalDateUtils.plus(LocalDateTime.now(), 30, ChronoUnit.SECONDS));
            taskDetailInfoBO.setExecutionStatus((byte) 1);
            taskDetailInfoBO.setTimeout(4000);
            taskDetailInfoBO.setRetryNum((byte) 3);
            list.add(taskDetailInfoBO);
        }
        taskDetailInfoService.batchInsert(list);
        System.in.read();
    }
}
