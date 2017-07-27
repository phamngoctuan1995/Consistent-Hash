import scala.collection.mutable.{HashMap, SortedSet}
import java.security.MessageDigest

class ConsistentHash[K, V](servers: List[V] = null, replicate: Int = 15)(implicit ordering: Ordering[V]) {
    var entries = HashMap.empty[V, List[Int]]
    var ring = SortedSet.empty[(Int, V)]
    private val random = new scala.util.Random(System.currentTimeMillis())
    var ringArray = ring.toArray

    val MAX_INT = 2147483647
    val MIN_INT = -2147483648

    if (servers != null)
        servers.foreach(server => add(server, replicate))

    private def stt() = {
        val count = HashMap.empty[V, Long]
        entries.keySet.foreach(key => count(key) = 0)
        count(ringArray(0)._2) += (ringArray(0)._1 - MIN_INT) + (MAX_INT - ringArray.last._1)
        for (i <- 1 until ringArray.length)
            count(ringArray(i)._2) += (ringArray(i)._1 - ringArray(i-1)._1)
        count.foreach(c => print(c._1 + " - " + c._2 + "  "))
        println()
    }

    private def NativeHash(str: Any, salt: Boolean = true): Int = {
        if (salt)
            return (random.nextInt() + "-" + str.toString).hashCode
        return str.toString.hashCode
    }

    private def JavaHash(hashType: String)(str: Any, salt: Boolean = true): Int = {
        if (salt)
            return MessageDigest.getInstance(hashType).digest((random.nextInt() + "-" + str.toString).getBytes).hashCode()
        return MessageDigest.getInstance(hashType).digest(str.toString.getBytes).toList.hashCode()
    }

    private def binarySearch(hCode: Int):V = {
        var left = 0
        var right = ringArray.length - 1
        var mid = 0

        if (hCode >= ringArray.last._1)
            return ringArray.head._2

        while (left <= right)
        {
            mid = (left + right) / 2
            if (ringArray(mid)._1 <= hCode)
                left = mid + 1
            else
                right = mid - 1
        }

        if (ringArray(mid)._1 <= hCode)
            return ringArray(mid + 1)._2


        return return ringArray(mid)._2
    }

    def add(server: V, replicate: Int):Unit = {
        if (replicate <= 0)
            return
        synchronized {
            if (entries.contains(server))
                return
            entries += server -> List.empty[Int]
            (1 to replicate).foreach(i => {
                val hCode = NativeHash(JavaHash("SHA-256")(i + "-" + server, false) + "-" + server, false)
                entries.update(server, hCode :: entries(server))
                ring.add(Tuple2(hCode, server))
            })

            ringArray = ring.toArray
        }
    }

    def add(server: V):Unit = {
        add(server, replicate)
    }

    def remove(server: V):Unit = {
        synchronized {
            if (!entries.contains(server))
                return
            entries(server).foreach(entry => ring.remove(Tuple2(entry, server)))
            entries.remove(server)
            ringArray = ring.toArray
        }
    }

    def get(key: K):V = {
        if (ringArray.length <= 0)
            throw new Exception("Consistent Hash is empty")
        val hCode = NativeHash("user-" + key, false)
        binarySearch(hCode)
    }
}

object Main {
    def main(args: Array[String]): Unit = {
        val ch = new ConsistentHash[Int, String](List("a", "b", "c", "d"))
    }
}
