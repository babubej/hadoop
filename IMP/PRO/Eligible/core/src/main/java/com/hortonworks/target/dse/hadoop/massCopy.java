package com.hortonworks.target.dse.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class massCopy {
	private static String sourceDir;
	private static String destDir;
	private static String fileSuffix;
	private static String keytab;

	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		if (args.length != 4) {
			throw new IllegalArgumentException("Expected 4 args");
		}
		sourceDir = args[0];
		destDir = args[1];
		fileSuffix = args[2];
		keytab = args[3];
		System.out.println("Source Dir: " + sourceDir);
		System.out.println("Dest Dir: " + destDir);
		System.out.println("File Suffix: " + fileSuffix);
		System.out.println("Keytab: " + keytab);
		
		Configuration conf = new Configuration();
		conf.set("hadoop.security.authentication", "Kerberos");
		UserGroupInformation.setConfiguration(conf);
		if(keytab.contains("stage")){
		    System.out.println("using stage keytab");
		    UserGroupInformation.loginUserFromKeytab("SVESERV@LITTLERED.TARGET.COM", keytab);
		}else{
		        System.out.println("using prod keytab");
		        UserGroupInformation.loginUserFromKeytab("SVESERV@BIGRED.TARGET.COM", keytab);
		}

		UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

		ExecutorService executorService = Executors.newFixedThreadPool(8);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] files = fs.listStatus(new Path(sourceDir));
		List<Future<Boolean>> outputs = new ArrayList<Future<Boolean>>();
		int i = 0;
		for (FileStatus file : files) {
			if (i % 100 == 0) {
				ugi.reloginFromKeytab();
			}
			i++;

			outputs.add(executorService.submit(new FileCopier(fs, file, conf)));
			//System.out.println("Moving file: " + file.getPath().toString());
			//FileUtil.copy(fs, file.getPath(), fs, new Path(destDir + file.getPath().getName() + fileSuffix), false, true, conf);
		}
		executorService.shutdown();
		
		if(!executorService.awaitTermination(20, TimeUnit.MINUTES)) {
			throw new IllegalStateException("Timeout occurred while copying files");
		}
		int failures = 0;
		for(Future<Boolean> output : outputs) {
			if(!output.get()) {
				failures++;
			}
		}
		if(failures == 0) {
			System.exit(0);
		} else {
			throw new IllegalStateException("Error copying files.  Number of failures: " + failures);
		}
	}

	public static class FileCopier implements Callable<Boolean> {
		private final FileSystem fs;
		private final FileStatus fileStatus;
		private final Configuration conf;

		@Override
		public Boolean call() throws Exception {
			System.out.println("Moving file: " + fileStatus.getPath().toString());
			System.out.println("Destination: " + new Path(destDir + fileStatus.getPath().toString()));
			boolean result = FileUtil.copy(fs, fileStatus.getPath(), fs, new Path(destDir + fileStatus.getPath().getName() + fileSuffix), false, true, conf);
			return result;
		}

		public  FileCopier(FileSystem fs, FileStatus fileStatus, Configuration conf) {
			this.fs = fs;
			this.fileStatus = fileStatus;
			this.conf = conf;
		}

	}
}
