import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

/*
aws_access_key_id = 
aws_secret_access_key = 
 */

public class WMSDBToS3Uploader {

	static final Logger logger = LoggerFactory.getLogger(WMSDBToS3Uploader.class);

	static String sBackupDir, sJdbcUrl, sDriver, sDbId, sDbPw, sDbPort, sDbName, sLogging, sAwsBucket, sAwsAccesKey, sAwsSecretKey;
	
	static boolean bUpdYn = false;
	
	static Connection conn = null;
	static PreparedStatement pstmt = null;
	static ResultSet rslt = null;
	
	static AWSCredentials awsCredentials = null; 
	static AmazonS3 amazonS3 = null;
	
	static ArrayList<String> arrFiles = new ArrayList<String>();
	
	public static void main(String[] args) {

		FileToProps(args[0]); // 기본적인 정보 로딩 
		GetFileLists(sBackupDir); // 업로드를 위한 파일 로딩 
		
		try {
			awsCredentials = new BasicAWSCredentials(sAwsAccesKey, sAwsSecretKey);
	        // amazonS3 = new AmazonS3Client(awsCredentials);
			
			/*
			amazonS3 = AmazonS3ClientBuilder
					   .standard()
					   .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
					   .withRegion(Regions.AP_NORTHEAST_2)
					   .build();
			*/
	        
			amazonS3 = AmazonS3ClientBuilder
					.standard()
					.withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
					.withEndpointConfiguration(
				        new AwsClientBuilder.EndpointConfiguration(
				                "https://bucket.vpce-0a415153acd6dae97-p1ldeu2p.s3.ap-northeast-2.vpce.amazonaws.com",
				                Regions.AP_NORTHEAST_2.getName())
					).build();
			
			File copyFile = null; 
	        		
	        if(arrFiles.size() > 0) {
	        	
	        	for(int i = 0; i < arrFiles.size(); i++) {
	        		copyFile = new File (arrFiles.get(i));
	        		
	        		FileUploadS3(copyFile);
	        	}
	        	
	        } else {
	        	System.out.println("File's Not Loading.");
	        }
 	        
		}catch(Exception e) {
			logger.debug("[Main WMSDBToS3Uploader Exception] : " + e.toString());
		}finally {
			try {
				if(rslt != null) {rslt.close();}
				if(pstmt != null) {pstmt.close();}
				if(conn != null) {conn.close();}
			}catch(Exception e) {
				logger.debug(e.toString());
			}
		}
	}


	public static void FileToProps(String sFile) {

		Properties prop = new Properties();
		InputStream is = null;

		try {
			is = new FileInputStream(sFile);
			prop.load(is);
			
			sBackupDir = prop.getProperty("BACKUP_DIR");
			sJdbcUrl = prop.getProperty("JDBC_URL");
			sDriver = prop.getProperty("JDBC_DRIVER");
			sDbId = prop.getProperty("DB_ID");
			sDbPw = prop.getProperty("DB_PW");
			sDbPort = prop.getProperty("DB_PORT");
			sDbName = prop.getProperty("DB_NAME");
			sLogging = prop.getProperty("LOGGING");
			sAwsBucket = prop.getProperty("AWS_BUCKETNAME");
			sAwsAccesKey = prop.getProperty("AWS_ACCESSKEY");
			sAwsSecretKey = prop.getProperty("AWS_SECRETKEY");
		} catch (Exception e) {
			logger.debug("[FileToProps_ReadInfoFile] : " + e.toString());
		} finally {
			try {
				if (is != null)
					is.close();
			} catch (Exception e) {
				logger.debug("[FileToProps_ReadInfoFile_Finally] : " + e.toString());
			}
		}
	}
	
	public static void GetFileLists (String sPath) {
		
		File dir = new File(sPath);
	    File files[] = dir.listFiles();

	    for (int i = 0; i < files.length; i++) {
	        File file = files[i];
	        
	        if(!file.isHidden()) { // hidden file except
	        	if (file.isDirectory()) {
		        	GetFileLists(file.getPath());
		        } else {
		            // System.out.println("file: " + file);
		            arrFiles.add(file.toString());
		        }
	        }
	    }
	}
		
	public static void FileUploadS3 (File srcFile) {
		
		try {
			
			String sMd5value = GetMD5Checksum(srcFile.toString());
			logger.debug("[FileUploadS3] CheckSum File : "+ srcFile.toString());
			logger.debug("[FileUploadS3] CheckSum Value : "+ sMd5value);
			
			String sCanontialName = srcFile.getCanonicalPath().substring(srcFile.getCanonicalPath().indexOf("/")+1);
			
			/*
			String sCanontialName = srcFile.getCanonicalPath().substring(srcFile.getCanonicalPath().indexOf("/")+1);
			
			PutObjectRequest putObjectRequest =
					new PutObjectRequest( sAwsBucket, sCanontialName, srcFile );
			        
			amazonS3.putObject(putObjectRequest);
			        
			bUpdYn = amazonS3.doesObjectExist( sAwsBucket,	sCanontialName); // 업로드 이후, 정상적으로 업로드 되었는지 파일 체크
			
			if(bUpdYn) {
				logger.debug("[FileUploadS3 Success] : " + sCanontialName);
				
				try {
					Class.forName(sDriver);
		        	conn = DriverManager.getConnection(sJdbcUrl, sDbId, sDbPw);
		        	pstmt = conn.prepareStatement("INSERT INTO S3UPLD_FLISTS (FSEQ, FILENAME) VALUES (FSEQ.NEXTVAL, ?)");
		        	pstmt.setString(1, srcFile.toString());
		        	rslt = pstmt.executeQuery();
				}catch(Exception e) {
					logger.debug("[FileUploadS3 DBException] : " + e.toString());
				}
			} else {
				logger.debug("[FileUploadS3 Fail] : " + sCanontialName);
			}
			*/
			
			TransferManager tm = TransferManagerBuilder.standard()
		                    .withS3Client(amazonS3)
		                    .build();
			
			Upload upload = tm.upload(sAwsBucket, sCanontialName, srcFile);
			logger.debug("[FileUploadS3] File Upload Start : " + sCanontialName);
			
			upload.waitForCompletion();
			
			if(upload.isDone()) {
				
				try {
					Class.forName(sDriver);
		        	conn = DriverManager.getConnection(sJdbcUrl, sDbId, sDbPw);
		        	pstmt = conn.prepareStatement("INSERT INTO S3UPLD_FLISTS (FSEQ, FILENAME) VALUES (FSEQ.NEXTVAL, ?)");
		        	pstmt.setString(1, srcFile.toString());
		        	rslt = pstmt.executeQuery();
				}catch(Exception e) {
					logger.debug("[FileUploadS3 DBException] : " + e.toString());
				}
			} else {
				logger.debug("[FileUploadS3 Fail] : " + sCanontialName);
			}
			
			logger.debug("[FileUploadS3] File Upload End : " + sCanontialName);
			
		}catch(Exception e) {
			logger.debug("[FileUploadS3 Exception] : " + e.toString());
		}
	}
	
	public static byte[] CreateChecksum(String filename) {
		
		InputStream fis = null;
		MessageDigest complete = null;
		
		try {
			fis = new FileInputStream(filename);

			byte[] buffer = new byte[1024];
			complete = MessageDigest.getInstance("MD5");
			int numRead;

			do {
				numRead = fis.read(buffer);
				if (numRead > 0) {
					complete.update(buffer, 0, numRead);
				}
			} while (numRead != -1);

			fis.close();
			
		}catch(Exception e) {
			logger.debug("[CreateChecksum Exception] : " + e.toString());
		}
		
		return complete.digest();
		
	}

	public static String GetMD5Checksum(String filename) {
		
		String result = "";
		
		try {
			byte[] b = CreateChecksum(filename);

			for (int i = 0; i < b.length; i++) {
				result += Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1);
			}
		}catch(Exception e) {
			logger.debug("[GetMD5Checksum Exception] : " + e.toString());
		}
		
		return result;
	}
	
}
