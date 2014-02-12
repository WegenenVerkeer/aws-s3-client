package be.wegenenverkeer.s3

/**
 * @author Karel Maesen, Geovise BVBA, 2014
 */



import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.*
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest

import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

abstract class s3Task {
    final AmazonS3 s3
    final String dir
    final String pattern
    final String bucket
    final String key

    s3Task(dir, pattern, bucket, key) {

        this.dir = dir
        this.pattern = pattern
        this.bucket = bucket
        this.key = key

        //create the s3 client
        s3 = new AmazonS3Client(new EnvironmentVariableCredentialsProvider())
        Region region = Region.getRegion(Regions.EU_WEST_1)
        s3.setRegion(region)
    }

    def run(Closure closure) {
        try {
            closure.call()
        } catch (AmazonServiceException ase) {
            println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            println("Error Message: " + ase.getMessage());
            println("HTTP Status Code: " + ase.getStatusCode());
            println("AWS Error Code: " + ase.getErrorCode());
            println("Error Type: " + ase.getErrorType());
            println("Request ID: " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            println("Error Message: " + ace.getMessage());
            println("Classpath: " + System.getProperty("java.class.path"))
        }
    }


}

class s3UploadTask extends s3Task {

    private final boolean deleteZipAfterUpload = false;

    public s3UploadTask(dir, pattern, bucket, key) {
        super(dir, pattern, bucket, key)
    }

    public run() {

        def zipFile = createZipFile()

        super.run{ t ->
            s3.putObject(new PutObjectRequest(bucket, key, zipFile));
        }

        if (deleteZipAfterUpload) {
            zipFile.delete()
        }
    }

    private createZipFile() {
        File outFile = File.createTempFile("s3-", ".tmp.zip")
        println "Creating temporary zip file " + outFile.absolutePath
        ZipOutputStream zipFile = new ZipOutputStream(new FileOutputStream(outFile))
        new File(dir).eachFileMatch(~pattern) { file ->
            zipFile.putNextEntry(new ZipEntry(file.getName()))
            zipFile << new FileInputStream(file)
            zipFile.closeEntry()
        }
        zipFile.close()
        return outFile
    }


}

class s3DownloadTask extends s3Task {

    private final boolean deleteZipAfterUnzip = true;

    public s3DownloadTask(dir, pattern, bucket, key) {
        super(dir, pattern, bucket, key)
    }

    public run() {
        println("Starting download for bucket: $bucket, key: $key")

        File outFile = File.createTempFile("s3-", ".tmp.zip")

        super.run{ t ->

            ObjectMetadata objMeta = s3.getObject( new GetObjectRequest(bucket, key ), outFile)
            println("Downloaded object of type: ${objMeta.contentType} and size: ${objMeta.contentLength}")
            println("Starting unzip.")
            unZipFile(outFile)
            println("Finished unzip.")
            println()

            if (deleteZipAfterUnzip) {
                println("Deleting zip file")
                outFile.delete()
            }

        }

    }

    private unZipFile(File zip) {

        def zipFile = new java.util.zip.ZipFile( zip )
        zipFile.entries().each { entry ->
           def inputStream = zipFile.getInputStream(entry)
           def outFile = new File(dir, entry.name)
           println("Writing ${outFile.path} ....")
           def outStream = new BufferedOutputStream( new FileOutputStream(outFile))
           int n;
           byte[] buffer = new byte[1024]
           while ( (n = inputStream.read(buffer)) > -1 ) {
               outStream.write(buffer, 0, n)
           }
           outStream.close()
           inputStream.close()
        }
    }

}
