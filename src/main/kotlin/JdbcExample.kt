import java.sql.Connection


fun main() {
    println("Run dirtyRead with TRANSACTION_READ_UNCOMMITTED")
    dirtyRead(Connection.TRANSACTION_READ_UNCOMMITTED)

    /*println("")
    println("Run dirtyRead with TRANSACTION_READ_COMMITTED")
    dirtyRead(Connection.TRANSACTION_READ_COMMITTED)

    println("")
    println("Run nonRepeatableRead with TRANSACTION_READ_COMMITTED")
    nonRepeatableRead(Connection.TRANSACTION_READ_COMMITTED)

    println("")
    println("Run nonRepeatableRead with TRANSACTION_REPEATABLE_READ")
    nonRepeatableRead(Connection.TRANSACTION_REPEATABLE_READ)

    println("")
    println("Run writeSkew with TRANSACTION_REPEATABLE_READ")
    writeSkew(Connection.TRANSACTION_REPEATABLE_READ)

    println("")
    println("Run writeSkew with TRANSACTION_SERIALIZABLE")
    try {
        writeSkew(Connection.TRANSACTION_SERIALIZABLE)
    } catch (e: Exception) {
        System.err.println("Error: " + e.message)
    }

    println("")
    println("Run writeSkew2 with TRANSACTION_SERIALIZABLE")
    try {
        writeSkew2(Connection.TRANSACTION_SERIALIZABLE)
    } catch(e: Exception) {
        System.err.println("Error: " + e.message)
    }

    println("")
    println("Run writeSkew3 with TRANSACTION_SERIALIZABLE")
    try {
        writeSkew3(Connection.TRANSACTION_SERIALIZABLE)
        println("writeSkew3 completed without error!")
    } catch (e: Exception) {
        System.err.println("Error: " + e.message)
    }*/
}

fun dirtyRead(transactionIsolation: Int) {
    val connection1 = newConnection(transactionIsolation)
    connection1.initDb()
    connection1.prepareStatement("UPDATE Account set balance = 0 where user_name = 'Alex';").execute()

    connection1.prepareStatement("START TRANSACTION;").execute()
    connection1.prepareStatement("UPDATE Account set balance = 10 where user_name = 'Alex';").execute()

    val connection2 = newConnection(transactionIsolation)
    val result = connection2.getBalance()

    connection1.prepareStatement("ROLLBACK").execute()

    println("Balance in connection2: $result")
    println("Real balance: ${newConnection(transactionIsolation).getBalance()}")
}

fun nonRepeatableRead(transactionIsolation: Int) {
    val connection1 = newConnection(transactionIsolation)
    connection1.initDb()
    connection1.prepareStatement("UPDATE Account set balance = 0 where user_name = 'Alex';").execute()

    val connection2 = newConnection(transactionIsolation)
    println("Connection2 START TRANSACTION")
    connection2.prepareStatement("START TRANSACTION;").execute()
    println("First read balance: " + connection2.getBalance())

    connection1.prepareStatement("START TRANSACTION;").execute()
    connection1.prepareStatement("UPDATE Account set balance = 10 where user_name = 'Alex';").execute()
    connection1.prepareStatement("COMMIT").execute()

    println("Second read balance: " + connection2.getBalance())
    println("Connection2 COMMIT TRANSACTION")
    connection2.prepareStatement("COMMIT").execute()
}

fun writeSkew(transactionIsolation: Int) {
    val connection1 = newConnection(transactionIsolation)
    connection1.initDb()
    connection1.prepareStatement("UPDATE Account set balance = 0 where user_name = 'Alex';").execute()
    println("Real balance: ${newConnection(transactionIsolation).getBalance()}")


    val connection2 = newConnection(transactionIsolation)
    connection2.prepareStatement("START TRANSACTION;").execute()
    val balance = connection2.getBalance() // lock s
    println("Connection2 get balance $balance")

    val connection = newConnection(transactionIsolation)
    connection.prepareStatement("START TRANSACTION;").execute()
    connection.prepareStatement(
        "UPDATE Account set balance = balance + 1 where user_name = 'Alex';"
    ).execute() // waiting connection2
    println("Connection1 increment balance")
    connection.prepareStatement("COMMIT").execute()

    connection2.prepareStatement("UPDATE Account set balance = ${balance + 1} where user_name = 'Alex';").execute()
    println("Connection2 increment balance")
    connection2.prepareStatement("COMMIT").execute()

    println("Real balance: ${newConnection(transactionIsolation).getBalance()}")
}

fun writeSkew2(transactionIsolation: Int) {
    val connection1 = newConnection(transactionIsolation)
    connection1.initDb()
    connection1.prepareStatement("UPDATE Account set balance = 0 where user_name = 'Alex';").execute()
    println("Real balance: ${newConnection(transactionIsolation).getBalance()}")


    val task = Runnable {
        val connection2 = newConnection(transactionIsolation)
        connection2.prepareStatement("START TRANSACTION;").execute()
        val balance = connection2.getBalance() // lock s
        println("Connection2 get balance $balance")
        Thread.sleep(2000)
        connection2.prepareStatement(
            "UPDATE Account set balance = ${balance + 1} where user_name = 'Alex';"
        ).execute() // lock x
        println("Connection2 increment balance")
        connection2.prepareStatement("COMMIT").execute()
    }

    val task2 = Runnable {
        Thread.sleep(1000)
        connection1.prepareStatement("START TRANSACTION;").execute()
        val balance2 = connection1.getBalance() // lock s
        connection1.prepareStatement(
            "UPDATE Account set balance = $balance2 + 1 where user_name = 'Alex';"
        ).execute() // lock x
        println("Connection1 increment balance")
        Thread.sleep(5000)
        connection1.prepareStatement("COMMIT").execute()
    }

    val thread = Thread(task)
    val thread2 = Thread(task2)
    thread.start()
    thread2.start()


    thread.join()
    thread2.join()
    println("Real balance: ${newConnection(transactionIsolation).getBalance()}")
}

fun writeSkew3(transactionIsolation: Int) {
    val connection1 = newConnection(transactionIsolation)
    connection1.initDb()
    connection1.prepareStatement("UPDATE Account set balance = 0 where user_name = 'Alex';").execute()
    println("Real balance: ${newConnection(transactionIsolation).getBalance()}")


    val task = Runnable {
        val connection2 = newConnection(transactionIsolation)
        connection2.prepareStatement("START TRANSACTION;").execute()

        val result = connection2.createStatement().executeQuery(
            "SELECT balance from Account FOR UPDATE"
        ) // lock x
        result.next()
        val balance = result.getInt("balance")
        println("Connection2 get balance $balance FOR UPDATE")

        Thread.sleep(2000)
        connection2.prepareStatement(
            "UPDATE Account set balance = ${balance + 1} where user_name = 'Alex';"
        ).execute()
        println("Connection2 increment balance")
        connection2.prepareStatement("COMMIT").execute()
    }

    val task2 = Runnable {
        Thread.sleep(1000)
        connection1.prepareStatement("START TRANSACTION;").execute()
        println("Connection1 tray get balance")
        val balance = connection1.getBalance() // waiting connection2
        println("Connection1 get balance $balance")
        connection1.prepareStatement(
            "UPDATE Account set balance = $balance + 1 where user_name = 'Alex';"
        ).execute()
        println("Connection1 increment balance")
        Thread.sleep(5000)
        connection1.prepareStatement("COMMIT").execute()
    }

    val thread = Thread(task)
    val thread2 = Thread(task2)
    thread.start()
    thread2.start()


    thread.join()
    thread2.join()
    println("Real balance: ${newConnection(transactionIsolation).getBalance()}")
}


