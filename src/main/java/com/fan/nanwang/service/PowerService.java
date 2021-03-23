package com.fan.nanwang.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.fan.nanwang.config.ZipDirectory;
import com.fan.nanwang.entity.CForecastPower;
import com.fan.nanwang.entity.ForecastPower;
import com.fan.nanwang.entity.PowerPlant;
import com.fan.nanwang.entity2.*;
import com.fan.nanwang.repository.PowerPlantRepository;
import com.fan.nanwang.repository2.*;
import com.fan.nanwang.utils.BeanMethodUtil;
import com.fan.nanwang.utils.ControlMaster;
import com.fan.nanwang.utils.FileUtil;
import com.fan.nanwang.utils.SlaveThread;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class PowerService {

    @Autowired
    private ZipDirectory zipDirectory;

    @Autowired
    private PowerPlantRepository powerPlantRepository;

    @Autowired
    private FarmInfoRepository farmInfoRepository;

    @Autowired
    private CdqResultRepository cdqResultRepository;

    @Autowired
    private DqResultRepository dqResultRepository;

    @Autowired
    private AreaInfoRepository areaInfoRepository;

    @Autowired
    private NetInfoRepository netInfoRepository;

    @Autowired
    private GpZlService gpZlService;

    /**
     * 保存实际功率数据到数据库
     */
    public void saveDataToDataBase(){
        // 注册管理对象
        ControlMaster controlMaster = new ControlMaster();
        zipDirectory.getList().forEach(region -> {
            PowerThread powerThread = new PowerThread(region);
            powerThread.joinMaster(controlMaster);
            powerThread.start();
        });
        //等待上面线程处理结束
        controlMaster.safeDone();
        // 保存岗坪、镇良光伏电站预测数据
        gpZlService.saveGpZlForecastData(getStartDateStrByRegion("guiz"));
        // 按地区、网省统计超短期预测值
        saveCForecastDataByNet();
        // 按地区、网省统计短期预测值
        saveForecastDataByNet();
    }

    private class PowerThread extends SlaveThread {
        private String region;
        PowerThread(String region) {
            this.region = region;
        }
        public void slaveRun() {
            log.info(region + "开始入库");
            Long startDateNum = getStartDateStrByRegion(region);
            // 遍历压缩包
            List<String> zipFileNameList = Arrays.asList(new File(zipDirectory.getZipPath() + "/" + region).list());
            // 过滤掉已入库数据文件
            zipFileNameList = zipFileNameList.stream().filter(zipName -> getDateNum(zipName) > startDateNum).collect(Collectors.toList());
            for(String zipName : zipFileNameList){
                log.info(region + ": " + (zipFileNameList.indexOf(zipName) + 1) + "/" + zipFileNameList.size());
                // 清空数据文件目录
                cleanDataFiles(region);
                // 解压数据压缩包到文件夹
                FileUtil.deCompressGZipFile(zipDirectory.getZipPath() + "/" + region + "/" + zipName, zipDirectory.getDirPath() + "/" + region);
                // 超短期预测数据入库
                saveForecastPowerData(region,"CDQYC");
                // 短期预测数据入库
                saveForecastPowerData(region,"DQYC");
            }
            log.info(region + "入库完成");
        }
    }

    Long getStartDateStrByRegion(String region){
        String objId = null;
        if("gd".equals(region)){
            objId = "NGD0002";
        }else if("gx".equals(region)){
            objId = "NGX0002";
        }else if("guiz".equals(region)){
            objId = "NGZ0002";
        }else if("hn".equals(region)){
            objId = "NHN0002";
        }else if("yn".equals(region)){
            objId = "NYN0002";
        }
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        try {
            Date maxDate = format.parse(cdqResultRepository.findMaxDateTimeByRegionObjId(objId));
            String dateStr = format.format(maxDate);
            dateStr = dateStr.replaceAll("-", "").replaceAll(":", "").replaceAll(" ", "");
            return Long.parseLong(dateStr);
        }catch (Exception e){
            return 0L;
        }
    }

    Long getDateNum(String str){
        Long dateNum = 0L;
        try {
            String dateStr = str.substring(str.indexOf("_xny_") + 5, str.lastIndexOf("_"));
            if(dateStr.length() == 14){
                dateStr = dateStr.substring(0, dateStr.length() - 2);
            }
            dateNum = Long.parseLong(dateStr);
        }catch (Exception e){
            log.error("压缩包名称提取日期失败: " + str);
        }
        return dateNum;
    }

    void saveForecastPowerData(String region, String label){
        File directory = new File(zipDirectory.getDirPath() + "/" + region);
        File[] files = directory.listFiles();
        for(File file : files){
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

    /**
     * 保存超短期预测数据
     */
    void saveCForecastPower(List<CForecastPower> cForecastPowerList){
        if(cForecastPowerList.size() <= 0){
            return;
        }
        CdqResult result = initCdqResult(cForecastPowerList);
        if(Objects.nonNull(result) && cdqResultRepository.findByObjIdAndDataTime(result.getObjId(), result.getDataTime()).size() == 0){
            cdqResultRepository.save(result);
        }
    }

    CdqResult initCdqResult(List<CForecastPower> cForecastPowerList){
        PowerPlant powerPlant = powerPlantRepository.findByNameAbbreviation(cForecastPowerList.get(0).getNameAbbreviation());
        if(Objects.isNull(powerPlant) || Objects.isNull(powerPlant.getYcId())){ return null; }
        FarmInfo farmInfo = farmInfoRepository.findByYcId(powerPlant.getYcId());
        if(Objects.isNull(farmInfo)){ return null; }
        CdqResult cdqResult = new CdqResult();
        cdqResult.setObjId(farmInfo.getFarmId());
        cdqResult.setObjType("FARM");
        cdqResult.setDataType("UP");
        cdqResult.setModelMark("9");
        cdqResult.setDataDate(cForecastPowerList.get(0).getDate().substring(0, 10));
        cdqResult.setDataTime(cForecastPowerList.get(0).getDate());
        cForecastPowerList.stream().forEach(cForecastPower -> {
            BeanMethodUtil.set(cdqResult, "setP" + cForecastPower.getDateSort(), cForecastPower.getForecastValue());
        });
        return cdqResult;
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
            DqResult result = initDqResult(data);
            if(Objects.nonNull(result) && dqResultRepository.findByObjIdAndDataDateAndPreDate(result.getObjId(), result.getDataDate(), result.getPreDate()).size() == 0){
                dqResultRepository.save(result);
            }
        }
    }

    DqResult initDqResult(List<ForecastPower> forecastPowerList){
        PowerPlant powerPlant = powerPlantRepository.findByNameAbbreviation(forecastPowerList.get(0).getNameAbbreviation());
        if(Objects.isNull(powerPlant) || Objects.isNull(powerPlant.getYcId())){ return null; }
        FarmInfo farmInfo = farmInfoRepository.findByYcId(powerPlant.getYcId());
        if(Objects.isNull(farmInfo)){ return null; }
        DqResult dqResult = new DqResult();
        dqResult.setObjId(farmInfo.getFarmId());
        dqResult.setObjType("FARM");
        dqResult.setDataType(farmInfo.getDataType());
        dqResult.setDataFlg("UP");
        dqResult.setDataDate(subDay(forecastPowerList.get(0).getDate().substring(0, 10)));
        dqResult.setDataTime("08:00:00");
        dqResult.setPreDate(forecastPowerList.get(10).getForecastDate().substring(0, 10));
        forecastPowerList.stream().forEach(forecastPower -> {
            String time = forecastPower.getForecastDate().substring(11).replace(":", "");
            time = "0000".equals(time)?"2400":time;
            BeanMethodUtil.set(dqResult, "setVal" + time, forecastPower.getForecastValue());
        });
        return dqResult;
    }

    String subDay(String dateStr){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date date = dateFormat.parse(dateStr);
            date.setDate(date.getDate() - 1);
            return dateFormat.format(date);
        }catch (Exception e){
            return dateStr;
        }
    }

    void saveCForecastDataByNet(){
        // 未按区域统计预测值的数据时间
        List<String> dataTimeList = cdqResultRepository.findDataTimeListByFarm();
        dataTimeList.stream().forEach(dataTime -> {
            saveCdqResultByArea(dataTime);
            saveCdqResultByNet(dataTime);
        });
        log.info("超短期预测数据按区域、网省统计预测值并入库完成");
    }

    void saveCdqResultByArea(String dataTime){
        // 按地区和风电光伏保存
        saveCdqAreaByRegion(dataTime);
        // 按网省和风电光伏保存
        saveCdqAreaByProvince(dataTime);
        // 按全网风电和光伏保存
        saveCdqAreaByAllArea(dataTime);
    }

    void saveCdqAreaByRegion(String dataTime){
        List<AreaInfo> areaInfoList = areaInfoRepository.findBypIdIsNotNull();
        areaInfoList.stream().forEach(areaInfo -> {
            // 当前区域下所有场站
            List<String> farmIdList = farmInfoRepository.findByAreaId(areaInfo.getAreaId()).stream().map(FarmInfo::getFarmId).collect(Collectors.toList());
            CdqResult cdqResult = JSON.parseObject(JSON.toJSONString(cdqResultRepository.getSumResultByDataTimeAndObjIdList(dataTime, farmIdList)), CdqResult.class);
            cdqResult.setObjId(areaInfo.getAreaId());
            cdqResult.setObjType("NET" + areaInfo.getDataType());
            if(Objects.nonNull(cdqResult.getDataDate())){
                cdqResultRepository.save(cdqResult);
            }
        });
    }

    void saveCdqAreaByProvince(String dataTime){
        List<AreaInfo> areaInfoList = areaInfoRepository.findBypIdIsNull();
        // ANW301、ANW302是南网全网地区编号
        areaInfoList.stream().filter(item -> !"ANW301".equals(item.getAreaId()) && !"ANW302".equals(item.getAreaId())).collect(Collectors.toList());
        areaInfoList.stream().forEach(areaInfo -> {
            List<String> areaIdList = areaInfoRepository.findBypId(areaInfo.getAreaId()).stream().map(AreaInfo::getAreaId).collect(Collectors.toList());
            CdqResult cdqResult = JSON.parseObject(JSON.toJSONString(cdqResultRepository.getSumResultByDataTimeAndObjIdList(dataTime, areaIdList)), CdqResult.class);
            cdqResult.setObjId(areaInfo.getAreaId());
            cdqResult.setObjType("NET" + areaInfo.getDataType());
            if(Objects.nonNull(cdqResult.getDataDate())){
                cdqResultRepository.save(cdqResult);
            }
        });
    }

    void saveCdqAreaByAllArea(String dataTime){
        List<AreaInfo> areaInfoList = areaInfoRepository.findBypIdIsNull();
        // 全网风电 ANW301
        List<String> allNet1IdList = areaInfoList.stream().filter(item -> !"ANW301".equals(item.getAreaId())
                && !"ANW302".equals(item.getAreaId())
                && item.getDataType() == 1).collect(Collectors.toList())
                .stream().map(AreaInfo::getAreaId).collect(Collectors.toList());
        CdqResult net1CdqResult = JSON.parseObject(JSON.toJSONString(cdqResultRepository.getSumResultByDataTimeAndObjIdList(dataTime, allNet1IdList)), CdqResult.class);
        net1CdqResult.setObjId("ANW301");
        net1CdqResult.setObjType("NET1");
        if(Objects.nonNull(net1CdqResult.getDataDate())){
            cdqResultRepository.save(net1CdqResult);
        }

        // 全网光伏 ANW302
        List<String> allNet2IdList = areaInfoList.stream().filter(item -> !"ANW301".equals(item.getAreaId())
                && !"ANW302".equals(item.getAreaId())
                && item.getDataType() == 2).collect(Collectors.toList())
                .stream().map(AreaInfo::getAreaId).collect(Collectors.toList());
        CdqResult net2CdqResult = JSON.parseObject(JSON.toJSONString(cdqResultRepository.getSumResultByDataTimeAndObjIdList(dataTime, allNet2IdList)), CdqResult.class);
        net2CdqResult.setObjId("ANW302");
        net2CdqResult.setObjType("NET2");
        if(Objects.nonNull(net2CdqResult.getDataDate())){
            cdqResultRepository.save(net2CdqResult);
        }
    }

    void saveCdqResultByNet(String dataTime){
        // 按网省和风电光伏保存
        saveCdqNetByProvince(dataTime);
        // 按全网风电和光伏保存
        saveCdqNetByAllArea(dataTime);
    }

    void saveCdqNetByProvince(String dataTime){
        List<NetInfo> netInfoList = netInfoRepository.findAll().stream().filter(item -> !"NNW0001".equals(item.getNetId())).collect(Collectors.toList());
        netInfoList.stream().forEach(netInfo -> {
            List<String> areaIdList = areaInfoRepository.findBypIdIsNullAndNetId(netInfo.getNetId()).stream().map(AreaInfo::getAreaId).collect(Collectors.toList());
            CdqResult cdqResult = JSON.parseObject(JSON.toJSONString(cdqResultRepository.getSumResultByDataTimeAndObjIdList(dataTime, areaIdList)), CdqResult.class);
            cdqResult.setObjId(netInfo.getNetId());
            cdqResult.setObjType("NET");
            if(Objects.nonNull(cdqResult.getDataDate())){
                cdqResultRepository.save(cdqResult);
            }
        });
    }

    void saveCdqNetByAllArea(String dataTime){
        List<String> netIdList = netInfoRepository.findAll().stream().filter(item -> !"NNW0001".equals(item.getNetId()))
                .collect(Collectors.toList())
                .stream().map(NetInfo::getNetId).collect(Collectors.toList());
        CdqResult cdqResult = JSON.parseObject(JSON.toJSONString(cdqResultRepository.getSumResultByDataTimeAndObjIdList(dataTime, netIdList)), CdqResult.class);
        cdqResult.setObjId("NNW0001");
        cdqResult.setObjType("NET");
        if(Objects.nonNull(cdqResult.getDataDate())){
            cdqResultRepository.save(cdqResult);
        }
    }

    void saveForecastDataByNet(){
        // 未按区域统计预测值的数据时间
        List<String> dataDateList = dqResultRepository.findDataDateListByFarm();
        dataDateList.stream().forEach(dateDate -> {
            saveDqResultByArea(dateDate);
            saveDqResultByNet(dateDate);
        });
        log.info("短期预测数据按区域、网省统计预测值并入库完成");
    }

    void saveDqResultByArea(String dateDate){
        // 按地区和风电光伏保存
        saveDqAreaByRegion(dateDate);
        // 按网省和风电光伏保存
        saveDqAreaByProvince(dateDate);
        // 按全网风电和光伏保存
        saveDqAreaByAllArea(dateDate);
    }

    void saveDqAreaByRegion(String dateDate){
        List<AreaInfo> areaInfoList = areaInfoRepository.findBypIdIsNotNull();
        areaInfoList.stream().forEach(areaInfo -> {
            // 当前区域下所有场站
            List<String> farmIdList = farmInfoRepository.findByAreaId(areaInfo.getAreaId()).stream().map(FarmInfo::getFarmId).collect(Collectors.toList());
            List<DqResult> dqResultList = JSONArray.parseArray(JSON.toJSONString(dqResultRepository.getSumResultByDataDateAndObjIdList(dateDate, farmIdList)), DqResult.class);
            dqResultList.forEach(dqResult -> {
                dqResult.setObjId(areaInfo.getAreaId());
                dqResult.setObjType("NET" + areaInfo.getDataType());
                dqResult.setDataType(areaInfo.getDataType());
                if(Objects.nonNull(dqResult.getDataDate())){
                    dqResultRepository.save(dqResult);
                }
            });
        });
    }

    void saveDqAreaByProvince(String dateDate){
        List<AreaInfo> areaInfoList = areaInfoRepository.findBypIdIsNull();
        // ANW301、ANW302是南网全网地区编号
        areaInfoList.stream().filter(item -> !"ANW301".equals(item.getAreaId()) && !"ANW302".equals(item.getAreaId())).collect(Collectors.toList());
        areaInfoList.stream().forEach(areaInfo -> {
            List<String> areaIdList = areaInfoRepository.findBypId(areaInfo.getAreaId()).stream().map(AreaInfo::getAreaId).collect(Collectors.toList());
            List<DqResult> dqResultList = JSONArray.parseArray(JSON.toJSONString(dqResultRepository.getSumResultByDataDateAndObjIdList(dateDate, areaIdList)), DqResult.class);
            dqResultList.forEach(dqResult -> {
                dqResult.setObjId(areaInfo.getAreaId());
                dqResult.setObjType("NET" + areaInfo.getDataType());
                dqResult.setDataType(areaInfo.getDataType());
                if(Objects.nonNull(dqResult.getDataDate())){
                    dqResultRepository.save(dqResult);
                }
            });
        });
    }

    void saveDqAreaByAllArea(String dateDate){
        List<AreaInfo> areaInfoList = areaInfoRepository.findBypIdIsNull();
        // 全网风电 ANW301
        List<String> allNet1IdList = areaInfoList.stream().filter(item -> !"ANW301".equals(item.getAreaId())
                && !"ANW302".equals(item.getAreaId())
                && item.getDataType() == 1).collect(Collectors.toList())
                .stream().map(AreaInfo::getAreaId).collect(Collectors.toList());
        List<DqResult> net1DqResultList = JSONArray.parseArray(JSON.toJSONString(dqResultRepository.getSumResultByDataDateAndObjIdList(dateDate, allNet1IdList)), DqResult.class);
        net1DqResultList.forEach(dqResult -> {
            dqResult.setObjId("ANW301");
            dqResult.setObjType("NET1");
            dqResult.setDataType(1);
            if(Objects.nonNull(dqResult.getDataDate())){
                dqResultRepository.save(dqResult);
            }
        });

        // 全网光伏 ANW302
        List<String> allNet2IdList = areaInfoList.stream().filter(item -> !"ANW301".equals(item.getAreaId())
                && !"ANW302".equals(item.getAreaId())
                && item.getDataType() == 2).collect(Collectors.toList())
                .stream().map(AreaInfo::getAreaId).collect(Collectors.toList());
        List<DqResult> net2DqResultList = JSONArray.parseArray(JSON.toJSONString(dqResultRepository.getSumResultByDataDateAndObjIdList(dateDate, allNet2IdList)), DqResult.class);
        net2DqResultList.forEach(dqResult -> {
            dqResult.setObjId("ANW302");
            dqResult.setObjType("NET2");
            dqResult.setDataType(2);
            if(Objects.nonNull(dqResult.getDataDate())){
                dqResultRepository.save(dqResult);
            }
        });
    }

    void saveDqResultByNet(String dateDate){
        // 按网省和风电光伏保存
        saveDqNetByProvince(dateDate);
        // 按全网风电和光伏保存
        saveDqNetByAllArea(dateDate);
    }

    void saveDqNetByProvince(String dateDate){
        List<NetInfo> netInfoList = netInfoRepository.findAll().stream().filter(item -> !"NNW0001".equals(item.getNetId())).collect(Collectors.toList());
        netInfoList.stream().forEach(netInfo -> {
            List<String> areaIdList = areaInfoRepository.findBypIdIsNullAndNetId(netInfo.getNetId()).stream().map(AreaInfo::getAreaId).collect(Collectors.toList());
            List<DqResult> dqResultList = JSONArray.parseArray(JSON.toJSONString(dqResultRepository.getSumResultByDataDateAndObjIdList(dateDate, areaIdList)), DqResult.class);
            dqResultList.forEach(dqResult -> {
                dqResult.setObjId(netInfo.getNetId());
                dqResult.setObjType("NET");
                dqResult.setDataType(0);
                if(Objects.nonNull(dqResult.getDataDate())){
                    dqResultRepository.save(dqResult);
                }
            });
        });
    }

    void saveDqNetByAllArea(String dateDate){
        List<String> netIdList = netInfoRepository.findAll().stream().filter(item -> !"NNW0001".equals(item.getNetId()))
                .collect(Collectors.toList())
                .stream().map(NetInfo::getNetId).collect(Collectors.toList());
        List<DqResult> dqResultList = JSONArray.parseArray(JSON.toJSONString(dqResultRepository.getSumResultByDataDateAndObjIdList(dateDate, netIdList)), DqResult.class);
        dqResultList.forEach(dqResult -> {
            dqResult.setObjId("NNW0001");
            dqResult.setObjType("NET");
            dqResult.setDataType(0);
            if(Objects.nonNull(dqResult.getDataDate())){
                dqResultRepository.save(dqResult);
            }
        });
    }

    /**
     * 删除处理目录下所有数据文件
     */
    void cleanDataFiles(String region){
        File file = new File(zipDirectory.getDirPath() + "/" + region);
        String[] content = file.list();
        for (String name : content) {
            File temp = new File(zipDirectory.getDirPath() + "/" + region, name);
            temp.delete();
        }
    }
}
