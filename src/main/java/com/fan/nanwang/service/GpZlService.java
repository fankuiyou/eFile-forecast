package com.fan.nanwang.service;

import com.fan.nanwang.config.ZipDirectory;
import com.fan.nanwang.entity.CForecastPower;
import com.fan.nanwang.entity.ForecastPower;
import com.fan.nanwang.entity2.CdqResult;
import com.fan.nanwang.entity2.DqResult;
import com.fan.nanwang.repository2.CdqResultRepository;
import com.fan.nanwang.repository2.DqResultRepository;
import com.fan.nanwang.utils.FileUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
@Slf4j
public class GpZlService {

    @Autowired
    private ZipDirectory zipDirectory;

    @Autowired
    private PowerService powerService;

    @Autowired
    private CdqResultRepository cdqResultRepository;

    @Autowired
    private DqResultRepository dqResultRepository;

    void saveGpZlForecastData(Long dateNum){
        // 从3小时前取数据文件
        dateNum = dateNum - 3000;
        log.info("开始镇良岗坪预测数据入库");
        // 短期预测数据入库
        saveForecastPowerData("CDQYC", "gangping", dateNum);
        // 超短期预测数据入库
        saveForecastPowerData("DQYC",  "gangping", dateNum);

        // 短期预测数据入库
        saveForecastPowerData("CDQYC", "zhenliang", dateNum);
        // 超短期预测数据入库
        saveForecastPowerData("DQYC", "zhenliang", dateNum);
        log.info("镇良岗坪预测数据入库完成");
    }

    void saveForecastPowerData(String label, String name, Long dateNum){
        File directory = new File(zipDirectory.getZipPath() + "/FDGF/" + name);
        File[] files = directory.listFiles();
        for(File file : files){
            // 跳过已入库的时间文件
            if(getDateNum(file.getName()).longValue() < dateNum){
                continue;
            }
            int labelIndex = file.getName().indexOf("_" + label + "_");
            if(labelIndex != -1){
                // 获取文件中的功率数据 -> 保存到数据库
                switch (label) {
                    case "CDQYC":
                        List<CForecastPower> cForecastPowerList = FileUtil.getCForecastPowerByFile(file, label);
                        saveCForecastPower(cForecastPowerList);
                        break;
                    case "DQYC":
                        List<ForecastPower> forecastPowerList = FileUtil.getForecastPowerByFile(file, label);
                        saveForecastPower(forecastPowerList);
                        break;
                }
            }
        }
    }

    Long getDateNum(String str){
        Long dateNum = 0L;
        try {
            String dateStr = str.substring(str.indexOf("DQYC_") + 5, str.lastIndexOf(".")).replaceAll("_", "");
            if(dateStr.length() == 14){
                dateStr = dateStr.substring(0, dateStr.length() - 2);
            }
            dateNum = Long.parseLong(dateStr);
        }catch (Exception e){
            log.error("e文件名称提取日期失败: " + str);
        }
        return dateNum;
    }

    /**
     * 保存超短期预测数据
     */
    void saveCForecastPower(List<CForecastPower> cForecastPowerList){
        if(cForecastPowerList.size() <= 0){
            return;
        }
        CdqResult result = powerService.initCdqResult(cForecastPowerList);
        if(Objects.nonNull(result) && cdqResultRepository.findByObjIdAndDataTime(result.getObjId(), result.getDataTime()).size() == 0){
            cdqResultRepository.save(result);
        }
    }

    /**
     * 保存短期预测数据
     */
    void saveForecastPower(List<ForecastPower> forecastPowerList){
        if(forecastPowerList.size() <= 0){
            return;
        }
        for(int i = 0; i < 3; i ++){
            List<ForecastPower> data = new ArrayList<>();
            for(int o = (i * 96); o < ((i + 1) * 96); o ++){
                data.add(forecastPowerList.get(o));
            }
            DqResult result = powerService.initDqResult(data);
            if(Objects.nonNull(result) && dqResultRepository.findByObjIdAndDataDateAndPreDate(result.getObjId(), result.getDataDate(), result.getPreDate()).size() == 0){
                dqResultRepository.save(result);
            }
        }
    }
}
