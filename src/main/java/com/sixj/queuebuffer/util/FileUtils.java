package com.sixj.queuebuffer.util;

import com.sixj.queuebuffer.factory.QueueBufferFactory;
import org.springframework.util.StringUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * <p>文件操作工具类<p>
 * @author sixiaojie
 * @date 2020-08-27-15:44
 */
@SuppressWarnings({"resource","unused"})
public class FileUtils {

    /**
     * 判断指定的文件是否存在。
     *
     * @param fileName
     * @return
     */
    public static boolean isFileExist(String fileName) {
        return new File(fileName).isFile();
    }

    /**
     * 以行为单位读取文件，读取到最后一行
     * @param filePath
     * @return
     */
    public static List<String> readFileContent(String filePath) {
        BufferedReader reader = null;
        List<String> listContent = new ArrayList<>();
        try {
            reader = new BufferedReader(new FileReader(filePath));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                listContent.add(tempString);
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return listContent;
    }


    /**
     * 文件数据写入（如果文件夹和文件不存在，则先创建，再写入）
     * @param filePath
     * @param content
     * @param flag true:如果文件存在且存在内容，则内容换行追加；false:如果文件存在且存在内容，则内容替换
     */
    public static void fileLinesWrite(String filePath,String content,boolean flag){
        FileWriter fw = null;
        try {
            File file=new File(filePath);
            //如果文件夹不存在，则创建文件夹
            if (!file.getParentFile().exists()){
                file.getParentFile().mkdirs();
            }
            //如果文件不存在，则创建文件,写入第一行内容
            if(!file.exists()){
                file.createNewFile();
                fw = new FileWriter(file);
            }else{//如果文件存在,则追加或替换内容
                fw = new FileWriter(file, flag);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        PrintWriter pw = new PrintWriter(fw);
        pw.println(content);
        pw.flush();
        try {
            fw.flush();
            pw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获得工程的WebRoot根目录
     * @return String
     */
    public static String getWebRootPath(Class clazz) {
        try {
            // classes 目录的物理路径
            String classpath = getClasspath(clazz);
            // WEB-INF 目录的物理路径
            String webInfoPath = new File(classpath).getParent();
            // WebRoot 目录的物理路径
            return new File(webInfoPath).getParent();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
    private static String getClasspath(Class clazz) {
        try {

            return clazz.getResource("/").getPath();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public static String getQueueBufferFilePath(String filepath){
        if(StringUtils.isEmpty(filepath)){
            filepath = getWebRootPath(QueueBufferFactory.class);
        }
        return filepath+File.separator+"queueTask.txt";
    }

    /**
     * 删除文件
     * @param filepath
     */
    public static void deleteFile(String filepath){
        File file = new File(filepath);
        if(file.exists()){
            file.delete();
        }
    }

    public static void main(String[] args) {
        System.out.println(isFileExist(getQueueBufferFilePath(null)));
    }
}