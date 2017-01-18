"""Display the distribution of sales by product name and product type."""
from context import sql
from data import sales, refund, product

product()
sales()
refund()


def distribution_of_sales_by_product_type():
    """Group sales by product type, sums the amount, and calculates the percent of distribution against the total amounts for all sales."""
    query = """
    SELECT
        p.product_type,
        SUM(s.total_quantity) AS total_quantity_sum,
        SUM(s.d_total_amount) AS total_amount_sum,
        (SUM(s.d_total_amount) / (SELECT SUM(d_total_amount) FROM sales) * 100) AS percent_distribution
    FROM product p
    INNER JOIN sales s ON p.product_id = s.product_id
    GROUP BY
        p.product_type
    ORDER BY
        total_amount_sum DESC
    """
    sql.sql(query).show()


def distribution_of_sales_by_product():
    """Group sales by product type and name, sums the amount, and calculates the percent of distribution against the total amounts for all sales."""
    query = """
    SELECT
        p.product_type,
        p.d_product_name,
        SUM(s.total_quantity) AS total_quantity_sum,
        SUM(s.d_total_amount) AS total_amount_sum,
        (SUM(s.d_total_amount) / (SELECT SUM(d_total_amount) FROM sales) * 100) AS percent_distribution
    FROM product p
    INNER JOIN sales s ON p.product_id = s.product_id
    GROUP BY
        p.product_type,
        p.d_product_name
    ORDER BY
        total_amount_sum DESC
    """
    sql.sql(query).show()

distribution_of_sales_by_product_type()
distribution_of_sales_by_product()
