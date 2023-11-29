import StringUtils.asHexString
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import java.io.InputStream
import java.security.MessageDigest



// TODO compare with https://commons.apache.org/proper/commons-codec/apidocs/index.html
object Hasher {
    private const val STREAM_BUFFER_LENGTH = 1024
    fun getCheckSumFromBytestream(inputStream: InputStream): String {
        val md = MessageDigest.getInstance(MessageDigestAlgorithm.SHA_256)
        val digest = updateDigestWithInputStream(md, inputStream).digest()
        val hexCode = asHexString(digest, true)
        return String(hexCode)
    }

    private fun updateDigestWithInputStream(digest: MessageDigest, data: InputStream): MessageDigest {
        val buffer = ByteArray(STREAM_BUFFER_LENGTH)
        var read = data.read(buffer, 0, STREAM_BUFFER_LENGTH)
        while (read > -1) {
            digest.update(buffer, 0, read)
            read = data.read(buffer, 0, STREAM_BUFFER_LENGTH)
        }
        return digest
    }
    object MessageDigestAlgorithm {
        const val MD2 = "MD2"
        const val MD5 = "MD5"
        const val SHA_1 = "SHA-1"
        const val SHA_224 = "SHA-224"
        const val SHA_256 = "SHA-256"
        const val SHA_384 = "SHA-384"
        const val SHA_512 = "SHA-512"
        const val SHA_512_224 = "SHA-512/224"
        const val SHA_512_256 = "SHA-512/256"
        const val SHA3_224 = "SHA3-224"
        const val SHA3_256 = "SHA3-256"
        const val SHA3_384 = "SHA3-384"
        const val SHA3_512 = "SHA3-512"
    }

}

object StringUtils {

    /**
     * Used to build output as Hex
     */
    private val DIGITS_LOWER =
        charArrayOf('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')

    /**
     * Used to build output as Hex
     */
    private val DIGITS_UPPER =
        charArrayOf('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')

    fun asHexString(data: ByteArray, toLowerCase: Boolean): CharArray {
        return asHexString(data, if (toLowerCase) DIGITS_LOWER else DIGITS_UPPER)
    }

    fun asHexString(data: ByteArray, toDigits: CharArray): CharArray {
        val l = data.size
        val out = CharArray(l shl 1)
        // two characters form the hex value.
        var i = 0
        var j = 0
        while (i < l) {
            out[j++] = toDigits[0xF0 and data[i].toInt() ushr 4]
            out[j++] = toDigits[0x0F and data[i].toInt()]
            i++
        }
        return out
    }
}

suspend fun <A, B> Iterable<A>.pmap(f: suspend (A) -> B): List<B> = coroutineScope {
    map { async { f(it) } }.awaitAll()
}