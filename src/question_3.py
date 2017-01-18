"""Calculate the total amount of all transactions that happened in year 2013 and have not been refunded as of today."""
from context import sql
from data import sales, refund

sales()
refund()


def not_refunded(year):
    """Sum the total amount of transactions that have not been refunded for the given year."""
    query = """
    SELECT
        SUM(s.d_total_amount) AS {year}_total_amount
    FROM sales s
    LEFT OUTER JOIN refund r ON  s.transaction_id = r.original_transaction_id
    WHERE
        YEAR(s.d_timestamp) = {year}
        AND r.refund_id IS NULL
    """.format(year=year)
    sql.sql(query).show()

not_refunded(2013)
