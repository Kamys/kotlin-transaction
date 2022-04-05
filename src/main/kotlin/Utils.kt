import java.sql.Connection
import java.sql.DriverManager

fun Connection.initDb() {
    val sqlCreateTable = """
        CREATE TABLE IF NOT EXISTS Account (
            user_name text not null,
            balance   int  not null
        );
    """.trimIndent()

    this.prepareStatement(sqlCreateTable).execute()

    val countResult = this.createStatement().executeQuery("SELECT count(*) as count FROM Account")
    countResult.next()
    val count = countResult.getInt("count")


    if (count == 0) {
        val sqlInsertAccount = """
        INSERT INTO Account (user_name, balance) VALUES ('Alex', 0);
    """.trimIndent()

        this.prepareStatement(sqlInsertAccount).execute()
    }
}

fun newConnection(transactionIsolation: Int): Connection {
    val connection = DriverManager
        .getConnection("jdbc:mysql://localhost:3306/kotlin-transaction", "root", "1234")
    connection.transactionIsolation = transactionIsolation
    return connection
}

fun Connection.getBalance(): Int {
    val result = this.createStatement().executeQuery("SELECT balance from Account")
    result.next()
    return result.getInt("balance")
}
