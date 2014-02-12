package be.wegenenverkeer.s3

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

        def options = cli.parse(args)

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
        def task = new s3UploadTask(options.d, options.p, options.b, options.k)
        task.run()
    }

    static def doDownload(options) {
        def task = new s3DownloadTask(options.d, options.p, options.b, options.k)
        task.run()
    }
}
