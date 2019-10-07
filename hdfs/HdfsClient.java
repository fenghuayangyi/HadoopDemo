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
	//���run config -> arguments ����
	// -DHADOOP_USER_NAME = bona
	public static void uploadToHdsfTest1() throws IOException{
		// 0 ��ȡ������Ϣ
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS","hdfs://bonacluster1:9000");

		// 1 ��ȡ�ļ�ϵͳ
		FileSystem fs = FileSystem.get(configuration);

		// 2 �����������ݵ���Ⱥ
		fs.copyFromLocalFile(new Path("E:/codetest/hadooptest/hello.txt"), new Path("/myhdfs/hello.txt"));

		// 3 �ر�fs
		fs.close();
		
		System.out.print("over");
	}
	
	public static void uploadToHdsfTest2() throws IOException, Exception{
		// 0 ��ȡ������Ϣ
		Configuration configuration = new Configuration();

		// 1 ��ȡ�ļ�ϵͳ
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");

		// 2 �����������ݵ���Ⱥ
		fs.copyFromLocalFile(new Path("E:/codetest/hadooptest/hello.txt"), new Path("/myhdfs/hello.txt"));

		// 3 �ر�fs
		fs.close();
		
		System.out.print("over");
	}

	
	//-----------------------------------API��ʽ
	//��ȡ�ļ�ϵͳ
	@Test
	public void getHdfs() throws Exception{
		// 0 ��ȡ������Ϣ
		Configuration configuration = new Configuration();

		// 1 ��ȡ�ļ�ϵͳ
		//FileSystem fs = FileSystem.get(configuration); //��ӡ�����ļ�ϵͳ������ڴ��ַ
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");//���ӡһЩ����ϸ����Ϣ
		
		// 3 ��ӡ�ļ�ϵͳ
		System.out.println(fs.toString());
	}
	
	//HDFS�ļ��ϴ�
	@Test
	public void putFileToHDFS() throws Exception{
		// 1 ����������Ϣ���� 
		// new Configuration();��ʱ�����ͻ�ȥ����jar���е�hdfs-default.xml
		// Ȼ���ټ���classpath�µ�hdfs-site.xml
		Configuration configuration = new Configuration();
		
		// 2 ���ò��� 
		// �������ȼ���
		//			1���ͻ��˴��������õ�ֵ  
		//			2��classpath�µ��û��Զ��������ļ� 
		//			3��Ȼ���Ƿ�������Ĭ������
		configuration.set("dfs.replication", "2");
		
		// 3 ��ȡ�ļ�ϵͳ
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");
		
		// 4 ����Ҫ�ϴ��ļ����ڵı���·��
		Path src = new Path("E:/codetest/hadooptest/hello.txt");
		
		// 5 ����Ҫ�ϴ���hdfs��Ŀ��·��
		Path dst = new Path("hdfs://bonacluster1:9000/myhdfs/input/hello.txt");
		
		// 6  ��ɿ����ļ����ر��ļ���
		fs.copyFromLocalFile(src, dst);
		fs.close();	
		System.out.print("over");

	}
	
	//HDFS�ļ�����
	@Test
	public void getFileFromHDFS() throws Exception{
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");
		// �����ļ� copyToLocalFile ��������  boolean delSrc ָ�Ƿ�ԭ�ļ�ɾ����ԭ·�� ��Ŀ��·��
		fs.copyToLocalFile(false,new Path("hdfs://bonacluster1:9000/myhdfs/hello.txt"), new Path("d:/hello.txt"));
		fs.close();
		System.out.print("over");
	}
	
	//HDFSĿ¼����
	@Test
	public void mkdirAtHDFS() throws Exception{
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");
		fs.mkdirs(new Path("hdfs://bonacluster1:9000/myhdfs/input"));
		fs.close();
		System.out.print("over");
	}
	
	//HDFS�ļ���ɾ��
	@Test
	public void deletedirAtHDFS() throws Exception{
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");
		// ɾ���ļ��� ������Ƿǿ��ļ��У�����2�Ƿ�ݹ�ɾ����true�ݹ�
		fs.delete(new Path("hdfs://bonacluster1:9000/myhdfs/output"),true);
		fs.close();
		System.out.print("over");
	}
	
	// �������ļ����ļ���
	@Test
	public void renameAtHDFS() throws Exception{
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");
		fs.rename(new Path("hdfs://bonacluster1:9000/myhdfs/rename.txt"),new Path("hdfs://bonacluster1:9000/myhdfs/hello.txt"));
		fs.close();
		System.out.print("over");
	}
	
	//HDFS�ļ����ļ����ж�
	@Test
	public void findAtHDFS() throws Exception, IllegalArgumentException, IOException{
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");
		// 2 ��ȡ��ѯ·���µ��ļ�״̬��Ϣ
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		// 3 ���������ļ�״̬
		for (FileStatus status : listStatus) {
			if (status.isFile()) {
				System.out.println("file--" + status.getPath().getName());
			} else {
				System.out.println(" dir--" + status.getPath().getName());
			}
		}
	}
	
	// HDFS�ļ�����鿴
	@Test
	public void readListFiles() throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");
		// ˼����Ϊʲô���ص�������������List֮�������
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
			System.out.println("--------------�ļ��ָ���--------------");
		}

	}

	
	//-----------------------------------IO����ʽ
	//ͨ��IO������HDFS �ļ��ϴ�
	@Test
	// ��λ���ص�һ������
	public void readFileSeek1() throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://bonacluster1:9000"), configuration, "bona");
		// 2 ��ȡ������·��
		Path path = new Path("hdfs://hadoop102:9000/user/atguigu/tmp/hadoop-2.7.2.tar.gz");
		// 3 ��������
		FSDataInputStream fis = fs.open(path);
		
	}

	//ͨ��IO������HDFS �ļ�����
	
	
	//ͨ��IO������HDFS ��λ�ļ���ȡ
	
	


}
