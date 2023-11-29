import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.serialization.ExperimentalSerializationApi


private const val defaultFileName = "diff-checksum-out.hash"
val logger = KotlinLogging.logger {}

suspend fun main(args: Array<String>) {
    val argMap: Map<String, String> = argsToArgMap(args) // todo add verbose and silent options (in addition to normal)

    val dir = getSourceDir(argMap)
    val destinationDir = getDestinationDir(argMap)
    val outputFile = getOutputFileArg(argMap)

    // todo react on file target instead
    if (destinationDir == null) { Comparator.fileOutputMode(dir, outputFile) }
    // todo if File output, provide an option to keep absolute paths
    // todo switch to command based model
    else Comparator.liveComparisonMode(dir, destinationDir)
}

private fun getOutputFileArg(argMap: Map<String, String>) = argMap["--output-file"] ?: defaultFileName

private fun getDestinationDir(argMap: Map<String, String>) = argMap["--destination"]

private fun getSourceDir(argMap: Map<String, String>) = argMap["--source"] ?: "./"

private fun argsToArgMap(args: Array<String>) =
    args.toList()
        .map { it.split("=") }
        .filter { (it.size == 2) }
        .associate { it[0] to it[1] }