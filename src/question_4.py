"""Display the customer name who made the second most purchases in the month of May 2013. Refunds should be excluded."""
from context import sql
from data import customer, sales, refund

customer()
sales()
refund()


def customer_with_second_most_purchases(year, month):
    """Display the name of the customer with the second most purchases for a given year and month."""
    query = """
    SELECT
        c.customer_first_name,
        c.customer_last_name
    FROM
    (
        SELECT
            customer_id,
            dense_rank() OVER (PARTITION BY year, month ORDER BY total_amount_sum DESC) as rank
        FROM(
            SELECT
                YEAR(s.timestamp) AS year,
                MONTH(s.timestamp) AS month,
                s.customer_id,
                SUM(s.d_total_amount) AS total_amount_sum
            FROM sales s
            LEFT OUTER JOIN refund r ON  s.transaction_id = r.original_transaction_id
            WHERE
                YEAR(s.d_timestamp) = {year}
                AND MONTH(s.d_timestamp) = {month}
                AND r.refund_id IS NULL
            GROUP BY
                YEAR(s.timestamp),
                MONTH(s.timestamp),
                s.customer_id
        )
    ) AS tmp
    INNER JOIN customer c ON tmp.customer_id = c.customer_id
    WHERE
        tmp.rank = 2
    """.format(year=year, month=month)
    sql.sql(query).show()


customer_with_second_most_purchases(2013, 5)
