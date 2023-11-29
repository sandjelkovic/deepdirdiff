import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.*
import kotlin.streams.asSequence
import kotlin.time.measureTimedValue

private const val s = "comparisonResult.diff"

// TODO convert to regular class and support DI for params
object Comparator {
    private val json = Json { prettyPrint = true }
    private const val comparisonResultOutputPath = "comparisonResult.diff"

    @OptIn(ExperimentalSerializationApi::class)
    suspend fun fileOutputMode(dir: String, outputFile: String) {
        logger.info { "Starting scan of ${dir.toFilePath().toAbsolutePath()}" }
        val fileCount = getFileCount(dir)

        val sourceRoot = dir.toFilePath()
        val sourceParsed = getDirChecksums(sourceRoot) // todo async
        val scanResult = (sourceParsed.mapKeys { it.key.toString() })

        writeScanResultToFile(outputFile, scanResult)

        logger.info { "Finished output to ${outputFile.toFilePath().toAbsolutePath()}" }
    }

    @OptIn(ExperimentalSerializationApi::class)
    private fun writeScanResultToFile(outputFile: String, scanResult: Map<String, String>) {
        outputFile.toFilePath().outputStream().use {
            json.encodeToStream(scanResult, it)
        }
    }

    private suspend fun getDirChecksums(checksumRoot: Path): Map<Path, String> {
        val (checksums, duration) = measureTimedValue {
            collectChecksumsRecursive(checksumRoot)
        }
        val (dirs, files) = checksums.partition { it.first.isDirectory() }
        logger.info { "Total of ${checksums.size} checksum calculated from $checksumRoot" }
        logger.info { "Directories checksums ${dirs.size} from $checksumRoot" }
        logger.info { "Files checksums ${files.size} from $checksumRoot" }
        logger.info { "Scan & calculation of source took ${duration.inWholeSeconds} seconds, or $duration" }
        // lists contain absolute paths but relative to the checksumRoot are needed for comparison
        return reduceToRelativePaths(checksums, checksumRoot)
    }

    suspend fun liveComparisonMode(dir: String, destinationDir: String) {
        logger.info { "Starting scan of ${dir.toFilePath().toAbsolutePath()}" }
        val fileCount = getFileCount(dir)
        logger.info { "Starting scan of ${destinationDir.toFilePath().toAbsolutePath()}" }
        val destFilesCount = getFileCount(destinationDir)

        val comparisonResult = compareDirs(dir.toFilePath(), destinationDir.toFilePath())
        logger.info { "Finished analysis" }
        logger.info { "Paths only found in source: " }
        comparisonResult.sourceOnlyPaths.forEach { path -> logger.info { path } }
        logger.info { "Paths only found in destination: " }
        comparisonResult.destinationOnlyPaths.forEach { path -> logger.info { path } }
        logger.info { "Different hash of files: " }
        comparisonResult.hashMismatchPaths.forEach { path -> logger.info { path } }
        // todo extract file path to variable/parameter
        writeComparisonResultToFile(comparisonResultOutputPath, comparisonResult)
        logger.info { "Comparison result written in $comparisonResultOutputPath" }
    }

    @OptIn(ExperimentalSerializationApi::class)
    private fun writeComparisonResultToFile(outputFile: String, comparisonResult: ComparisonResult) =
        outputFile.toFilePath().outputStream().use {
            json.encodeToStream(comparisonResult, it)
        }


    private suspend fun getFileCount(dir: String): Long {
        val (fileCount, time) = measureTimedValue { countFilesRecursive(Paths.get(dir)) }
        logger.info { "$dir has $fileCount files measured in $time" }
        return fileCount
    }

    suspend fun compareDirs(sourceRoot: Path, destinationRoot: Path): ComparisonResult {
        if (!sourceRoot.exists() || !destinationRoot.exists()) throw RuntimeException("Paths do not exist")

//    if (!Files.isDirectory(sourceRoot)) checkFile(sourceRoot) // check if both are dirs or files & compare
        // assume dirs for now
        return coroutineScope {
            val (sourceResult, destinationResult) = async { getDirChecksums(sourceRoot) } to async {
                getDirChecksums(
                    destinationRoot
                )
            }
            measureTimedValue { analyseDiffs(sourceResult.await(), destinationResult.await()) }
                .also { timedResult -> logger.info { "Result comparison took ${timedResult.duration}" } }.value
        }
    }

    private fun reduceToRelativePaths(
        checksums: List<Pair<Path, String>>,
        rootPath: Path,
    ): Map<Path, String> {
        return checksums.toMap().mapKeys {
            if (!it.key.toAbsolutePath().equals(rootPath.toAbsolutePath())) {
                it.key.toAbsolutePath().subpath(rootPath.toAbsolutePath().nameCount, it.key.toAbsolutePath().nameCount)
            } else {
                "/".toFilePath()
            }
        }
    }

    // TODO streamChecksums
    private fun analyseDiffs(
        source: Map<Path, String>,
        destination: Map<Path, String>,
    ): ComparisonResult = ComparisonResult(sourceOnlyPaths = source.keys.subtract(destination.keys).toList(),
        destinationOnlyPaths = destination.keys.subtract(source.keys).toList(),
        hashMismatchPaths = source.keys.intersect(destination.keys)
            .filter { !source[it].contentEquals(destination[it]) })

    // TODO Can be loaded from files too, like analysis files
    suspend fun collectChecksumsRecursive(path: Path): List<Pair<Path, String>> = when {
        path.isDirectory() -> {
            val dirEntriesChecksum = path.listDirectoryEntries().pmap { collectChecksumsRecursive(it) }.flatten()
                .sortedBy { it.first.toString() }
            dirEntriesChecksum + calculateDirChecksum(
                path,
                dirEntriesChecksum
            ).also { dirChecksumResult -> logger.info { "${dirChecksumResult.first} parsed" } } // TODO also pushes update into a channel which prints or does calculation of dirs based on total size
        } // TODO Potentially OOM with joiningToString checksumList()

        path.isRegularFile() || path.isHidden() -> {
            listOf(path to calculateFileChecksum(path))
        }

        else -> {
            listOf(path to "")
        }
    }

    private fun calculateDirChecksum(
        path: Path,
        checksumList: List<Pair<Path, String>>,
    ) = (path to Hasher.getCheckSumFromBytestream(checksumList.joinToString { it.second }.byteInputStream()))

    private suspend fun calculateFileChecksum(path: Path): String = withContext(Dispatchers.IO) {
        try {
            path.inputStream().use { Hasher.getCheckSumFromBytestream(it) }
        } catch (e: Exception) {
            logger.error { "Error while checksuming $path" }
            e.printStackTrace()
            throw e
        }
    }

    @Serializable
    data class ComparisonResult(
        val sourceOnlyPaths: List<Path>,
        val destinationOnlyPaths: List<Path>,
        val hashMismatchPaths: List<Path>,
    )

    @Serializable
    data class ScanResult(
        val values: Map<String, String>,
    )

    fun String.toFilePath() = Path.of(this)

    private suspend fun countFilesRecursive(path: Path): Long = when {
        !path.exists() -> 0
        !path.isDirectory() -> 1
        else -> {
            val (first, second) = withContext(Dispatchers.IO) { Files.list(path) }.asSequence()
                .partition { it.isDirectory() }
            second.size + first.sumOf { countFilesRecursive(it) }
        }
    }
}