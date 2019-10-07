package com.bona.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class HdfsClient {
	//
	public static void main(String[] args) throws Exception {
//		uploadToHdsfTest1();
		uploadToHdsfTest2();
	}
	
	//Permission denied: user=Administrator, access=WRITE
	//添加run config -> arguments 输入
	// -DHADOOP_USER_NAME = bona
	public static void uploadToHdsfTest1() throws IOException{
		// 0 获取配置信息
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS","hdfs://bonacluster1:9000");

		// 1 获取文件系统
		FileSystem fs = FileSystem.get(configuration);

		// 2 拷贝本地数据到集群
		fs.copyFromLocalFile(new Path("E:/codetest/hadooptest/hello.txt"), new Path("/myhdfs/hello.txt"));

		// 3 关闭fs
		fs.close();
		
		System.out.print("over");
	}
	
	public static void uploadToHdsfTest2() throws IOException, Exception{
		// 0 获取配置信息
		Configuration configuration = new Configuration();

		// 1 获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");

		// 2 拷贝本地数据到集群
		fs.copyFromLocalFile(new Path("E:/codetest/hadooptest/hello.txt"), new Path("/myhdfs/hello.txt"));

		// 3 关闭fs
		fs.close();
		
		System.out.print("over");
	}

	
	//-----------------------------------API方式
	//获取文件系统
	@Test
	public void getHdfs() throws Exception{
		// 0 获取配置信息
		Configuration configuration = new Configuration();

		// 1 获取文件系统
		//FileSystem fs = FileSystem.get(configuration); //打印的是文件系统对象的内存地址
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");//会打印一些更详细的信息
		
		// 3 打印文件系统
		System.out.println(fs.toString());
	}
	
	//HDFS文件上传
	@Test
	public void putFileToHDFS() throws Exception{
		// 1 创建配置信息对象 
		// new Configuration();的时候，它就会去加载jar包中的hdfs-default.xml
		// 然后再加载classpath下的hdfs-site.xml
		Configuration configuration = new Configuration();
		
		// 2 设置参数 
		// 参数优先级：
		//			1、客户端代码中设置的值  
		//			2、classpath下的用户自定义配置文件 
		//			3、然后是服务器的默认配置
		configuration.set("dfs.replication", "2");
		
		// 3 获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");
		
		// 4 创建要上传文件所在的本地路径
		Path src = new Path("E:/codetest/hadooptest/hello.txt");
		
		// 5 创建要上传到hdfs的目标路径
		Path dst = new Path("hdfs://bonacluster1:9000/myhdfs/input/hello.txt");
		
		// 6  完成拷贝文件并关闭文件流
		fs.copyFromLocalFile(src, dst);
		fs.close();	
		System.out.print("over");

	}
	
	//HDFS文件下载
	@Test
	public void getFileFromHDFS() throws Exception{
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");
		// 下载文件 copyToLocalFile 三个参数  boolean delSrc 指是否将原文件删除，原路径 ，目标路径
		fs.copyToLocalFile(false,new Path("hdfs://bonacluster1:9000/myhdfs/hello.txt"), new Path("d:/hello.txt"));
		fs.close();
		System.out.print("over");
	}
	
	//HDFS目录创建
	@Test
	public void mkdirAtHDFS() throws Exception{
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");
		fs.mkdirs(new Path("hdfs://bonacluster1:9000/myhdfs/input"));
		fs.close();
		System.out.print("over");
	}
	
	//HDFS文件夹删除
	@Test
	public void deletedirAtHDFS() throws Exception{
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");
		// 删除文件夹 ，如果是非空文件夹，参数2是否递归删除，true递归
		fs.delete(new Path("hdfs://bonacluster1:9000/myhdfs/output"),true);
		fs.close();
		System.out.print("over");
	}
	
	// 重命名文件或文件夹
	@Test
	public void renameAtHDFS() throws Exception{
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");
		fs.rename(new Path("hdfs://bonacluster1:9000/myhdfs/rename.txt"),new Path("hdfs://bonacluster1:9000/myhdfs/hello.txt"));
		fs.close();
		System.out.print("over");
	}
	
	//HDFS文件和文件夹判断
	@Test
	public void findAtHDFS() throws Exception, IllegalArgumentException, IOException{
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");
		// 2 获取查询路径下的文件状态信息
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		// 3 遍历所有文件状态
		for (FileStatus status : listStatus) {
			if (status.isFile()) {
				System.out.println("file--" + status.getPath().getName());
			} else {
				System.out.println(" dir--" + status.getPath().getName());
			}
		}
	}
	
	// HDFS文件详情查看
	@Test
	public void readListFiles() throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");
		// 思考：为什么返回迭代器，而不是List之类的容器
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/myhdfs"), true);
		while (listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
				
			System.out.println(fileStatus.getPath().getName());
			System.out.println(fileStatus.getBlockSize());
			System.out.println(fileStatus.getPermission());
			System.out.println(fileStatus.getLen());
				
			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
				
			for (BlockLocation bl : blockLocations) {
					
				System.out.println("block-offset:" + bl.getOffset());
					
				String[] hosts = bl.getHosts();
					
				for (String host : hosts) {
					System.out.println(host);
				}
			}
			System.out.println("--------------文件分割线--------------");
		}

	}

	
	//-----------------------------------IO流方式
	//通过IO流操作HDFS 文件上传
	@Test
	// 定位下载第一块内容
	public void readFileSeek1() throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");
		// 2 获取输入流路径
		Path path = new Path("hdfs://hadoop102:9000/user/atguigu/tmp/hadoop-2.7.2.tar.gz");
		// 3 打开输入流
		FSDataInputStream fis = fs.open(path);
		
	}

	//通过IO流操作HDFS 文件下载
	
	
	//通过IO流操作HDFS 定位文件读取
	
	


}
