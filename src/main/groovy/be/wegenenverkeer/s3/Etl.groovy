package be.wegenenverkeer.s3

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.regions.*
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.AmazonS3EncryptionClient
import com.amazonaws.services.s3.model.CryptoConfiguration
import com.amazonaws.services.s3.model.EncryptionMaterials
import com.amazonaws.services.s3.model.StaticEncryptionMaterialsProvider

/**
 * @author Karel Maesen, Geovise BVBA, 2014
 */
class Etl {

    def static main (args) {

        def cli = new CliBuilder(usage:'etl <options> <cmd>')
        cli.t(longOpt: 'task', args: 1, argName: 'UP | DOWN', 'upload or download')
        cli.b(longOpt: 'bucket', args: 1, argName: 'bucket', 'S3 bucket')
        cli.k(longOpt: 'key', args:1 , argName: 'key', 'object key')
        cli.d(longOpt: 'dir', args:1 , argName: 'dir', 'input or output directory')
        cli.p(longOpt: 'pattern', args:1 , argName: 'pattern', 'file input pattern')
        cli.h(longOpt: 'help', 'print usage')
        cli.PH(longOpt: 'proxyhost', args:1 , argName: 'proxyhost', 'proxy host')
        cli.PP(longOpt: 'proxyport', args:1 , argName: 'proxyport', 'proxy port')
        cli.SK(longOpt: 'secretkey:', args:1, argName: 'privatekey', 'private encryption key')
        cli.PK(longOpt: 'publickey:', args:1, argName: 'publickey', 'public encryption key')

        def options = cli.parse(args)

        if (options == null){
            //cli parse error
            return
        }
        if (options.h) {
            println(cli.usage())
            return
        }

        if ('UP'.equalsIgnoreCase(options.t)) {
            doUpload(options)
        } else {
            doDownload(options)
        }

    }

    static def doUpload(options) {
        def client = clientBuilder(options)
        def task = new s3UploadTask(client, options.d, options.p, options.b, options.k)
        task.run()
    }

    static def doDownload(options) {
        def client = clientBuilder(options)
        def task = new s3DownloadTask(client, options.d, options.p, options.b, options.k)
        task.run()
    }

    static def clientBuilder(options){
        println("Starting client builder")
        ClientConfiguration clientConfiguration = new ClientConfiguration()

        if(options.PH) {
            clientConfiguration.setProxyHost(options.PH)
        }

        if(options.PP) {
            clientConfiguration.setProxyPort(Integer.parseInt(options.PP))
        }

        def s3 = null
        if(options.SK || options.PK ){
            println"Using Client-side encryption with ${options.SK} / ${options.PK}"
            def keyPair = KeyLoader.loadKeyPair(options.SK ? options.SK : null, options.PK)
            def materials = new EncryptionMaterials(keyPair)
            s3 = new AmazonS3EncryptionClient(new EnvironmentVariableCredentialsProvider(),
                    new StaticEncryptionMaterialsProvider(materials), clientConfiguration, new CryptoConfiguration())
        } else {
            //create the s3 client
            s3 = new AmazonS3Client(new EnvironmentVariableCredentialsProvider(), clientConfiguration)
        }
        Region region = Region.getRegion(Regions.EU_WEST_1)
        s3.setRegion(region)
        return s3
    }
}
