package com.fan.nanwang.utils;

import com.fan.nanwang.entity.CForecastPower;
import com.fan.nanwang.entity.ForecastPower;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

@Slf4j
public class FileUtil {

    public static String getRegion(String content, String label){
        int fromIndex = content.indexOf("<" + label + "::") + (3 + label.length());
        int endIndex = content.indexOf(".", fromIndex);
        String region = content.substring(fromIndex, endIndex);
        if("GZ".equalsIgnoreCase(region)){
            region = "GuiZ";
        }
        if(region.length() > 10){
            return "";
        }else{
            return region;
        }
    }

    /**
     * 解压压缩包
     * @param tarGzFile
     * @param destDir
     */
    public static void deCompressGZipFile(String tarGzFile, String destDir) {
        // 建立输出流，用于将从压缩文件中读出的文件流写入到磁盘
        TarArchiveEntry entry = null;
        TarArchiveEntry[] subEntries = null;
        File subEntryFile = null;
        try (FileInputStream fis = new FileInputStream(tarGzFile);
             GZIPInputStream gis = new GZIPInputStream(fis);
             TarArchiveInputStream taris = new TarArchiveInputStream(gis);) {
            while ((entry = taris.getNextTarEntry()) != null) {
                StringBuilder entryFileName = new StringBuilder();
                entryFileName.append(destDir).append(File.separator).append(entry.getName());
                File entryFile = new File(entryFileName.toString());
                if (entry.isDirectory()) {
                    if (!entryFile.exists()) {
                        entryFile.mkdir();
                    }
                    subEntries = entry.getDirectoryEntries();
                    for (int i = 0; i < subEntries.length; i++) {
                        try (OutputStream out = new FileOutputStream(subEntryFile)) {
                            subEntryFile = new File(entryFileName + File.separator + subEntries[i].getName());
                            IOUtils.copy(taris, out);
                        } catch (Exception e) {
                            log.error("deCompressing file failed:" + subEntries[i].getName() + "in" + tarGzFile);
                        }
                    }
                } else {
                    checkFileExists(entryFile);
                    OutputStream out = new FileOutputStream(entryFile);
                    IOUtils.copy(taris, out);
                    out.close();
                    //如果是gz文件进行递归解压
                    if (entryFile.getName().endsWith(".gz")) {
                        deCompressGZipFile(entryFile.getPath(), destDir);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("decompress failed", e);
        }
    }

    public static void checkFileExists(File file) {
        //判断是否是目录
        if (file.isDirectory()) {
            if (!file.exists()) {
                file.mkdir();
            }
        } else {
            //判断父目录是否存在，如果不存在，则创建
            if (file.getParentFile() != null && !file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            try {
                file.createNewFile();
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }
    }

    public static Map<String, Object> getFileData(File file, String label){
        InputStreamReader in = null;
        BufferedReader br = null;
        try {
            in = new InputStreamReader(new FileInputStream(file),"GBK");
            br = new BufferedReader(in);
            StringBuffer content = new StringBuffer();
            String s = "";
            while ((s=br.readLine())!=null){
                content = content.append(s);
            }
            // 场站所属地区
            String region = getRegion(content.toString(), label);
            String dataStr = content.substring(content.indexOf("<" + label), content.indexOf("</" + label));
            String date = dataStr.substring(dataStr.indexOf("Date='") + 6, dataStr.indexOf("Date='") + 16);
            String time = dataStr.substring(dataStr.indexOf("Time='") + 6, dataStr.indexOf("Time='") + 11);
            time = time.replace("-", ":");
            // 数据日期
            String dateTime = date + " " + time;
            int minute = Integer.parseInt(dateTime.substring(14));
            minute = minute - (minute % 5);
            if(minute < 10){
                dateTime = dateTime.substring(0, 14) + "0" + minute;
            }else{
                dateTime = dateTime.substring(0, 14) + minute;
            }
            // 场站名称缩写
            String nameAbbreviation = dataStr.substring(dataStr.indexOf("<" + label) + label.length() + 3, dataStr.indexOf("\tDate"));
            dataStr = dataStr.substring(dataStr.indexOf("#"), dataStr.length());

            String[] dataList = dataStr.split("#\t");
            // 如果有多条数据
            if(dataList.length > 2){
                Map<String, Object> ret = new HashMap<>();
                ret.put("dateTime", dateTime);
                ret.put("region", region);
                ret.put("nameAbbreviation", nameAbbreviation);
                ret.put("data", dataList);
                return ret;
            }else{
                return null;
            }
        }catch (Exception e){
            log.error(e.getMessage());
            return null;
        }finally {
            if(in != null){
                try {
                    in.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            if(br != null){
                try {
                    br.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }

    public static List<CForecastPower> getCForecastPowerByFile(File file, String label) {
        List<CForecastPower> cForecastPowerList = new ArrayList<>();
        Map<String, Object> map = getFileData(file, label);
        if(map != null){
            String region = map.get("region") + "";
            String dateTime = map.get("dateTime") + "";
            String nameAbbreviation = map.get("nameAbbreviation") + "";
            String[] dataList = (String[]) map.get("data");
            try {
                for(int i = 1; i < dataList.length; i ++){
                    String[] data = dataList[i].split("\t");
                    CForecastPower cForecastPower = new CForecastPower();
                    cForecastPower.setRegion(region);
                    cForecastPower.setNameAbbreviation(nameAbbreviation);
                    cForecastPower.setDateSort(Integer.parseInt(data[0]));
                    cForecastPower.setDate(dateTime);
                    cForecastPower.setForecastDate(getForecastDateById(dateTime, Integer.parseInt(data[0])));
                    cForecastPower.setForecastValue(data[1]);
                    cForecastPower.setVolume(data[2]);
                    cForecastPowerList.add(cForecastPower);
                }
            }catch (Exception e){
                log.error(nameAbbreviation + ": " + e.getMessage());
            }
        }
        return cForecastPowerList;
    }

    public static List<ForecastPower> getForecastPowerByFile(File file, String label) {
        List<ForecastPower> forecastPowerList = new ArrayList<>();
        Map<String, Object> map = getFileData(file, label);
        if(map != null){
            String region = map.get("region") + "";
            String dateTime = map.get("dateTime") + "";
            String nameAbbreviation = map.get("nameAbbreviation") + "";
            String[] dataList = (String[]) map.get("data");
            for(int i = 1; i < dataList.length; i ++){
                String[] data = dataList[i].split("\t");
                ForecastPower forecastPower = new ForecastPower();
                forecastPower.setRegion(region);
                forecastPower.setNameAbbreviation(nameAbbreviation);
                forecastPower.setDateSort(Integer.parseInt(data[0]));
                forecastPower.setDate(dateTime);
                forecastPower.setForecastDate(getForecastDateById(dateTime, Integer.parseInt(data[0])));
                forecastPower.setForecastValue(data[1]);
                forecastPower.setVolume(data[2]);
                try {forecastPower.setRunNum(data[3]);}catch (Exception e){}
                forecastPowerList.add(forecastPower);
            }
        }
        return forecastPowerList;
    }

    public static String getForecastDateById(String dateStr, int id){
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        try {
            Date date = format.parse(dateStr);
            date.setMinutes(date.getMinutes() + (id - 1) * 15);
            return format.format(date);
        }catch (Exception e){
            return "";
        }
    }
}
