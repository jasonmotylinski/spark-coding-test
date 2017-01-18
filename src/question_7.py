"""Display all the details of a customer who is currently living at 1154 Winters Blvd."""
from context import sql
from data import customer_extended

customer_extended()


def customers_living_at(address):
    """Display all the details of a customer who is currently living at the given address."""
    query = """
    SELECT
       LOWER(current_street_address),
       COUNT(*) AS ct
    FROM
        customer_extended
    WHERE
        LOWER(current_street_address) = '{address}'
    GROUP BY
        current_street_address,
        id
    """.format(address=address.lower())
    sql.sql(query).show()

customers_living_at('1154 Winters Blvd')
