"""Calculate the total number of users who purchased the same product consecutively at least 2 times on a given day."""
from context import sql
from data import sales, refund, product

product()
sales()
refund()


def total_users_who_purchased_same_product():
    """Calculate the total number of users who purchased the same product consecutively at least 2 times on a given day."""
    query = """
    SELECT
        COUNT(*) AS ct
    FROM
    (
        SELECT
            customer_id,
            ct
        FROM
        (
            SELECT
                YEAR(d_timestamp) AS year,
                MONTH(d_timestamp) AS month,
                DAY(d_timestamp) AS day,
                customer_id,
                product_id,
                COUNT(*) AS ct
            FROM sales
            GROUP BY
                YEAR(d_timestamp),
                MONTH(d_timestamp),
                DAY(d_timestamp),
                customer_id,
                product_id
        )
        WHERE
            ct >= 2
    )
    """
    sql.sql(query).show()

total_users_who_purchased_same_product()
