"""Find a product that has not been sold at least once (if any)."""
from context import sql
from data import sales, refund, product

product()
sales()
refund()


def attempt_1():
    """Find products that are not in the sales table."""
    query = """
        SELECT
            p.product_id
        FROM product p
        LEFT OUTER JOIN sales s on p.product_id = s.product_id
        WHERE s.transaction_id IS NULL
    """
    sql.sql(query).show()


def attempt_2():
    """Find products that have a sale but the quantity is zero."""
    query = """
        SELECT
            p.product_id,
            SUM(s.total_quantity) AS total_quantity_sum
        FROM product p
        INNER JOIN sales s on p.product_id = s.product_id
        WHERE s.total_quantity < 1
        GROUP BY
            p.product_id
        ORDER BY
            total_quantity_sum ASC
    """
    sql.sql(query).show()


def attempt_3():
    """Find products which have a transaction, but may have had a refund, thus negating the sales."""
    query = """
    SELECT
        p.product_id,
        tmp.total_amount_sum
    FROM product p
    INNER JOIN
    (
        SELECT
            s.product_id,
            SUM(s.d_total_amount) AS total_amount_sum
        FROM sales s
        LEFT OUTER JOIN refund r on s.transaction_id = r.original_transaction_id
        WHERE
            r.original_transaction_id IS NULL
            AND s.d_total_amount < 1
        GROUP BY
            s.product_id
    ) tmp ON p.product_id = tmp.product_id
    """
    sql.sql(query).show()

attempt_1()
attempt_2()
attempt_3()
