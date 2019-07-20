package com.cdh.test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.testng.annotations.Test;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
public class test {
    /**
     * 通过fileSystem获取分布式文件系统的几种方式
     */
    //获取hdfs分布式文件系统的第一种方式
    @Test
    public void getFileSystem1() throws IOException {
        //如果configuration 不做任何配置，获取到的是本地文件系统
        Configuration configuration = new Configuration();
        //覆盖我们的hdfs的配置，得到我们的分布式文件系统
        configuration.set("fs.defaultFS","hdfs://192.168.52.100:8020/");
        FileSystem fileSystem = FileSystem.get(configuration);
        System.out.println(fileSystem.toString());
    }
    /**
     * 获取hdfs分布式文件系统的第二种方式
     */
    @Test
    public void getHdfs2() throws URISyntaxException, IOException {
        //使用两个参数来获取hdfs文件系统
        //第一个参数是一个URI，定义了我们使用hdfs://这种方式来访问，就是分布式文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.52.100:8020"), new Configuration());
        System.out.println(fileSystem.toString());
    }
    /**
     * 获取hdfs分布式文件系统的第三种方式
     */
    @Test
    public  void getHdfs3() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.52.100:8020");
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        System.out.println(fileSystem.toString());
    }
    /**
     * 获取hdfs分布式文件系统的第四种方式
     */
    @Test
    public  void getHdfs4() throws Exception {
        //使用两个参数来获取hdfs文件系统
        //第一个参数是一个URI，定义了我们使用hdfs://这种方式来访问，就是分布式文件系统
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://192.168.52.100:8020"), new Configuration());
        System.out.println(fileSystem.toString());
    }
    /**
     * hdfs上面创建文件夹
     */
    @Test
    public void createHdfsDir() throws  Exception{
        //获取分布式文件系统的客户端对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.52.100:8020"), new Configuration());
        fileSystem.mkdirs(new Path("/abc/bbc/ddd"));
        fileSystem.close();

    }
    /**
     * hdfs的文件上传
     */
    @Test
    public void uploadFileToHdfs() throws  Exception{
        //获取分布式文件系统的客户端
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.52.100:8020"), new Configuration());
        //通过copyFromLocalFile 将我们的本地文件上传到hdfs上面去
        fileSystem.copyFromLocalFile(false,new Path("file:///f:\\平凡的世界.txt"),new Path("/abc/bbc/ddd"));
        fileSystem.close();
    }
//遍历hdfs上面所有的文件

    @Test
    public void listHdfsFiles() throws  Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.52.100:8020"), new Configuration());
        Path path = new Path("/");
        //alt  +  shift  +  l  提取变量
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(path, true);
        //遍历迭代器，获取我们的迭代器里面每一个元素
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            Path path1 = next.getPath();
            System.out.println(path1.toString());
        }
        fileSystem.close();
    }

}
