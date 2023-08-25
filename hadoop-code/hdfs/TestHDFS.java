package com.zzj.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestHDFS {
    public Configuration conf = null;
    public FileSystem fs = null;

    @Before
    public void conn() throws Exception {
    	// 使用hadoop接口，根据放置的hdfs的配置文件（fs.defaultFS是hdfs的话返回dfs）+环境变量HADOOP_USER_NAME中的用户 创建fs对象
        conf = new Configuration(true);
        fs = FileSystem.get(conf);  // 会报个异常 把异常抛出去  父类引用会指向一个子类实现
        
        //或者自己指定用户和文件系统
        fs = FileSystem.get(URI.create("hdfs://mycluster"), conf, "god";

    }

    @After
    public void close() throws Exception {
        fs.close();
    }

    @Test
    public void mkdir() throws Exception {

        Path dir = new Path("/zzj");
        if(fs.exists(dir)){
            fs.delete(dir, true);
        }
        fs.mkdirs(dir);
    }
                            
    @Test
    public void upload() throws Exception {

        // 指定文件的输入和输出  将本地的文件上传到HDFS
        BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File("./data/hello.txt")));
        Path outfile = new Path("/zzj/out.txt");
        FSDataOutputStream output = fs.create(outfile);

        IOUtils.copyBytes(input, output, conf, true);
    }

    @Test
    public void  blocks() throws Exception {
        //  块即文件 HDFS的特性：可以并行处理不同块的内容 示例：打印不同块

        //1. 获取并打印HDFS的元数据
        Path file = new Path("/zzj/out.txt");
        FileStatus fss = fs.getFileStatus(file);
        BlockLocation[] blks = fs.getFileBlockLocations(fss, 0, fss.getLen());
        for (BlockLocation b: blks){
            System.out.println(b);
        }
        //2. 读取对应数据  会读一个个字符 指针读完后移
        FSDataInputStream in = fs.open(file);
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());

        //3. 给定元数据中的偏移量 此时重新运行读取 直接会从第二个块开始读
        in.seek(1048576);

        // 距离机制：如果要计算的DN数据在多台机器上  计算程序会优先用本机（距离最近）的DN上的数据

    }
}
