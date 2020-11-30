package com.liuwenxu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Copyright (C), 2015-2020, https://www.liuwenxu.com/
 * FileName: HdfsClient
 * Author: liuwenxu
 * Date: 2020/11/30 下午5:03
 * Description: hdfs api 测试
 */
public class HdfsClient {

    private FileSystem fs;
    String basePath = "/Users/liuwenxu/code/idea/hadoop-learn/src/main/java/com/liuwenxu/";

    @Before
    public void getFS() {

        try {
            fs = FileSystem.get(URI.create(
                    "hdfs://hadoop130:8020"),
                    new Configuration(),
                    "atlwx");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * 文件上传
     **/
    @Test
    public void put() throws IOException {
        fs.copyFromLocalFile(new Path("/Users/liuwenxu/code/idea/hadoop-learn/src/main/resources/log4j2.xml"), new Path("/"));
    }

    /**
     * 文件下载
     **/
    @Test
    public void get() throws IOException {
        fs.copyToLocalFile(false, new Path("/tmp"), new Path("/Users/liuwenxu/code/idea/hadoop-learn/src/main/resources/"));
    }

    /**
     * 创建目录
     **/
    @Test
    public void makeDir() throws IOException {
        fs.mkdirs(new Path("/lwx"));
    }

    /**
     * 删除文件夹
     **/
    @Test
    public void removeDir() throws IOException {
        boolean delete = fs.delete(new Path("/lwx/test/logs/work"), true);
        System.out.println(delete);

    }

    /**
     * 修改文件名
     **/
    @Test
    public void update() throws IOException {
        fs.rename(new Path("/log4j2.xml"), new Path("/log4j.xml"));
    }

    /**
     * 查看文件详情
     **/
    @Test
    public void fileStatus() throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
        while (iterator.hasNext()) {
            LocatedFileStatus next = iterator.next();
            Path path = next.getPath();
            String owner = next.getOwner();
            System.out.println(path.toString() + "-----" + owner);

            BlockLocation[] blockLocations = next.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                System.out.println("offset：" + blockLocation.getOffset());

                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("host：" + host);
                }

                String[] names = blockLocation.getNames();
                for (String name : names) {
                    System.out.println("name：" + name);

                }
            }
            System.out.println("---------------------------");
        }
    }

    /**
     * 查看文件夹
     **/
    @Test
    public void dirStatus() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                Path path = status.getPath();
                String owner = status.getOwner();
                System.out.println(path.toString() + "-----" + owner);
            } else {
                System.out.println(status.getPath().toString());
            }
        }
    }

    /**
     * io上传文件
     **/
    @Test
    public void putByIO() throws Exception {
        // creat inputStream
        FileInputStream is = new FileInputStream(basePath + "HdfsClient.java");
        // creat outputStream
//        FSDataOutputStream os = fs.create(new Path("/java/HdfsClient.java"));
        FSDataOutputStream os = fs.append(new Path("/java/HdfsClient.java"));
        //copy
        try {
            IOUtils.copyBytes(is, os, 1024, false);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            // close stream
            IOUtils.closeStream(is);
            IOUtils.closeStream(os);
        }
    }

    /**
     * io下载文件
     **/
    @Test
    public void getByIO() throws Exception {
        // 输入流
        FSDataInputStream is = fs.open(new Path("/log4j.xml"));
        //对接
//        FileOutputStream os = new FileOutputStream(basePath + "1.java");
        try {
//            IOUtils.copyBytes(is, os, 1024, false);
            IOUtils.copyBytes(is, System.out, 1024, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(is);
//            IOUtils.closeStream(os);
        }
    }

    @After
    public void close() {
        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
