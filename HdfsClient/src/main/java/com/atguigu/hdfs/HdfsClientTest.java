package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Scanner;


/**
 * 1. 和HDFS建立连接
 * 2. 调用API完成具体功能
 * 3. 关闭连接   关闭连接   关闭连接   关闭连接  
 */
public class HdfsClientTest {
    private  FileSystem fs;


    /**
     * 上传文件
     * 测试配置的优先级 Configuration > hdfs-site.xml > hdfs-default
     * @throws IOException
     */
    @Test
    public void testCopyFromLocal() throws IOException {
        fs.copyFromLocalFile(false,true,
                           new Path("E:\\temp\\bigdata\\hello.txt"),
                           new Path("/client_test"));
    }


    /**
     * 删除文件和目录
     * @throws IOException
     */
    @Test
    public void testDelete() throws IOException {
        fs.delete(new Path("/client_test/hello.txt"),true);
    }



    /**
     * 下载文件
     * @throws IOException
     */
    @Test
    public void testCopyToLocal() throws IOException {
        fs.copyToLocalFile(false,new Path("/client_test/hello.txt"),
                new Path("E:\\temp\\bigdata\\download"),true);
    }

    /**
     * 文件更名或移动
     * @throws IOException
     */
    @Test
    public void testRename() throws IOException {
//        fs.rename(new Path("/sanguo/liubei.txt"),new Path("/client_test"));
        fs.rename(new Path("/client_test/liubei.txt"),new Path("/client_test/xiaoqiao.txt"));
    }

    /**
     * 查看文件详情
     * @throws IOException
     */
    @Test
    public void testListFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()){
            System.out.println("文件名:"+locatedFileStatusRemoteIterator.next().getPath().getName());
            System.out.println("副本数:"+locatedFileStatusRemoteIterator.next().getReplication());
            System.out.println("块大小:"+locatedFileStatusRemoteIterator.next().getBlockSize());
            System.out.println("权限:"+locatedFileStatusRemoteIterator.next().getPermission());
        }
    }

    /**
     * 判断是文件还是目录
     * @throws IOException
     */
    @Test
    public void testListStatus() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus status: fileStatuses) {
            if(status.isDirectory()){
                System.out.println("dir:"+status.getPath().getName());
            }else{
                System.out.println("file:"+status.getPath().getName());
            }
        }
    }

    /**
     * 获取 FileSystem 对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Before
    public void init() throws IOException, InterruptedException{
        /**
         * 参数
         * uri : HDFS的访问路径 hdfs://hadoop102:9820  (namenode)
         * conf : 配置对象
         * user ： 操作用户（用哪个用户操作HDFS）
         */
        URI uri = URI.create("hdfs://hadoop102:9820");
        Configuration conf = new Configuration();
        conf.set("dfs.replication","6");
        String user = "atguigu";
        /**
         * 获取HDFS客户端连接对象(文件系统对象)
         */
        fs = FileSystem.get(uri, conf, user);
        System.out.println(fs.getClass().getName());

    }

    @After
    public void close() throws IOException {
        /**
         * 关闭资源
         */
        fs.close();
    }

    @Test
    public void testCreateHdfsClent() throws IOException, InterruptedException {

    }

    @Test
    public void  giveMeBangbangtang(){
        System.out.println("张梓玥的愿望是：给我一个棒棒糖！！！");
        System.out.println("好吧，张梓玥这么乖，我就给你一个棒棒糖吧");
        System.out.println("棒棒糖拿去");
        System.out.println("给我一个钟雪糕");
    }



    public static void main(String[] args) throws Exception{
//        //创建方法
//        Scanner sc=new Scanner(System.in);
//        System.out.println("张梓玥，你的愿望是什么");
//        System.out.println("我想要玩具和好吃的");
//        System.out.println("好的，你输入1的时候就能得到一个爱莎；\n 你输入2的时候就可以得到一袋薯片；\n 输入3的时候就可以得到一个毛绒玩具");
//        //获取输入的数字
//        int num= sc.nextInt();
//        if(num == 1){
//            System.out.println("给你一个爱莎");
//        }else if(num == 2){
//            System.out.println("给你一袋薯片");
//        }else{
//            System.out.println("给你一个毛绒玩具");
//        }
        test();
    }

    public static void ReadTest(){

        System.out.println("ReadTest, Please Enter Data:");

        InputStreamReader is = new InputStreamReader(System.in); //new构造InputStreamReader对象

        BufferedReader br = new BufferedReader(is); //拿构造的方法传到BufferedReader中

        try{ //该方法中有个IOExcepiton需要捕获

            String name = br.readLine();

            System.out.println("ReadTest Output:" + name);

        }

        catch(IOException e){

            e.printStackTrace();

        }

    }


    public static void ScannerTest(){

        Scanner sc = new Scanner(System.in);

        System.out.println("ScannerTest, Please Enter Name:");

        String name = sc.nextLine(); //读取字符串型输入

        System.out.println("ScannerTest, Please Enter Age:");

        int age = sc.nextInt(); //读取整型输入

        System.out.println("ScannerTest, Please Enter Salary:");

        float salary = sc.nextFloat(); //读取float型输入

        System.out.println("Your Information is as below:");

        System.out.println("Name:" + name +"\n" + "Age:"+age + "\n"+"Salary:"+salary);

    }

    public static void test(){
        int a;
        int b;
        int c;
        Scanner s = new Scanner(System.in);
        String str = s.next();
        String[] arr = str.split("/");
        a = Integer.parseInt(arr[0]);
        b = Integer.parseInt(arr[1]);
        c = Integer.parseInt(arr[2]);
        System.out.println("a="+a+",b="+b+",c="+c);
    }

}
