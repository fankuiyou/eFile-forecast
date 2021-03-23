package com.fan.nanwang.timeTask;

import com.fan.nanwang.config.ZipDirectory;
import com.fan.nanwang.service.PowerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.*;

@Slf4j
@Component
public class PowerTask {

    @Autowired
    private ZipDirectory zipDirectory;

    @Autowired
    private PowerService powerService;

    @Value("${task.power.enable}")
    private boolean enabled;

    @Scheduled(cron = "${task.power.cron}")
    public void saveDataToDataBase(){
        if (!enabled) { return; }
        // 通过oss接口获取实时数据文件
        executeLinuxCmd("/home/dky/ossutil64 cp -r oss://os2-wjzz/xny/ " + zipDirectory.getZipPath() + " -u");
        // 岗坪镇良光伏电站预测数据入库
        executeLinuxCmd("/home/dky/ossutil64 cp -r oss://os2-wjzz/FDGF/ " + zipDirectory.getZipPath() + "/FDGF -u --include \"*DQYC_*\"");
        // 解析数据文件并入库
        powerService.saveDataToDataBase();
    }

    void executeLinuxCmd(String cmd) {
        log.info("got cmd job : " + cmd);
        Runtime run = Runtime.getRuntime();
        try {
            Process process = run.exec(cmd);
            InputStream in = process.getInputStream();
            BufferedReader bs = new BufferedReader(new InputStreamReader(in));
            String result = null;
            while ((result = bs.readLine()) != null) {
                log.info("job result [" + result + "]");
            }
            in.close();
            process.destroy();
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }
}
