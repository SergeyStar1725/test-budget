package mobi.sevenwinds.app.budget

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import mobi.sevenwinds.app.author.AuthorEntity
import mobi.sevenwinds.app.author.AuthorTable
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.transaction

object BudgetService {
    suspend fun addRecord( param: BudgetAuthorParam, body: BudgetRecord,): BudgetRecord = withContext(Dispatchers.IO) {
        transaction {
            var author: AuthorEntity? = null;

            val query = AuthorTable.select{ AuthorTable.id eq param.id_author}
            author = AuthorEntity.wrapRow(query.first())
            val entity = BudgetEntity.new {
                this.year = body.year
                this.month = body.month
                this.amount = body.amount
                this.type = body.type
                this.author = author
            }

            return@transaction entity.toResponse()
        }
    }

    suspend fun getYearStats(param: BudgetYearParam): BudgetYearStatsResponse = withContext(Dispatchers.IO) {
        transaction {
            val query1 = BudgetTable
                .select { BudgetTable.year eq param.year }

            //получение всех записей для указанного года
            val total = query1.count()
            val data1 = BudgetEntity.wrapRows(query1).map { it.toResponse() }

            //получение записей по запросу limit и offset
            val query2 = query1.limit(param.limit, param.offset)
            val data2 = BudgetEntity.wrapRows(query2).map { it.toResponse() }

            //сортировка по возрастанию месяца и убывания суммы
            val sortedData = data2.sortedWith(compareBy<BudgetRecord> { it.month }.thenByDescending { it.amount })

            //подсчёт статистики по всем записям
            val sumByType = data1.groupBy { it.type.name }.mapValues { it.value.sumOf { v -> v.amount } }

            return@transaction BudgetYearStatsResponse(
                total = total,
                totalByType = sumByType,
                items = sortedData
            )
        }
    }
}