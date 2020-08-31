from sqlalchemy.sql import expression, func, distinct, text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import Numeric, String, Integer

# A custom age function for postgres


class postgres_age(expression.FunctionElement):
    type = Numeric()
    name = "postgres_age"


@compiles(postgres_age)
def pgage(element, compiler, **kw):
    # Calculate the difference in years, then adjust based on whether the current date has passed
    # using EXTRACT('dayofyear') failed on edge cases
    clauses = compiler.process(element.clauses)
    return (
        "DATEDIFF('YEAR', %s, CURRENT_DATE) - " % clauses
        + "CASE WHEN extract('month' from CURRENT_DATE) + extract('day' from CURRENT_DATE)/100.0 "
        + "< extract('month' from %s)+ extract('day' from %s)/100.0 THEN 1 ELSE 0 END"
        % (clauses, clauses)
    )


# Custom function definitions for bigquery


class bq_median(expression.FunctionElement):
    type = Numeric()
    name = "bq_median"


class bq_percentile1(expression.FunctionElement):
    type = Numeric()
    name = "bq_percentile1"


class bq_percentile5(expression.FunctionElement):
    type = Numeric()
    name = "bq_percentile5"


class bq_percentile10(expression.FunctionElement):
    type = Numeric()
    name = "bq_percentile10"


class bq_percentile25(expression.FunctionElement):
    type = Numeric()
    name = "bq_percentile25"


class bq_percentile75(expression.FunctionElement):
    type = Numeric()
    name = "bq_percentile75"


class bq_percentile90(expression.FunctionElement):
    type = Numeric()
    name = "bq_percentile90"


class bq_percentile95(expression.FunctionElement):
    type = Numeric()
    name = "bq_percentile95"


class bq_percentile99(expression.FunctionElement):
    type = Numeric()
    name = "bq_percentile99"


@compiles(bq_median, "bigquery")
def bqmedian(element, compiler, **kw):
    return "approx_quantiles(%s, 2)[OFFSET(1)]" % compiler.process(element.clauses)


@compiles(bq_percentile1, "bigquery")
def bqpercentile1(element, compiler, **kw):
    return "approx_quantiles(%s, 100)[OFFSET(1)]" % compiler.process(element.clauses)


@compiles(bq_percentile5, "bigquery")
def bqpercentile5(element, compiler, **kw):
    return "approx_quantiles(%s, 20)[OFFSET(1)]" % compiler.process(element.clauses)


@compiles(bq_percentile10, "bigquery")
def bqpercentile10(element, compiler, **kw):
    return "approx_quantiles(%s, 10)[OFFSET(1)]" % compiler.process(element.clauses)


@compiles(bq_percentile25, "bigquery")
def bqpercentile25(element, compiler, **kw):
    return "approx_quantiles(%s, 4)[OFFSET(1)]" % compiler.process(element.clauses)


@compiles(bq_percentile75, "bigquery")
def bqpercentile75(element, compiler, **kw):
    return "approx_quantiles(%s, 4)[OFFSET(3)]" % compiler.process(element.clauses)


@compiles(bq_percentile90, "bigquery")
def bqpercentile90(element, compiler, **kw):
    return "approx_quantiles(%s, 10)[OFFSET(9)]" % compiler.process(element.clauses)


@compiles(bq_percentile95, "bigquery")
def bqpercentile95(element, compiler, **kw):
    return "approx_quantiles(%s, 20)[OFFSET(19)]" % compiler.process(element.clauses)


@compiles(bq_percentile99, "bigquery")
def bqpercentile99(element, compiler, **kw):
    return "approx_quantiles(%s, 100)[OFFSET(99)]" % compiler.process(element.clauses)


# An age calculation for bigquery


class bq_age(expression.FunctionElement):
    type = Numeric()
    name = "bq_age"


@compiles(bq_age, "bigquery")
def bqage(element, compiler, **kw):
    clauses = compiler.process(element.clauses)
    return (
        "DATE_DIFF(CURRENT_DATE, %s, YEAR) - " % clauses
        + "IF(EXTRACT(MONTH FROM CURRENT_DATE) + EXTRACT(DAY FROM CURRENT_DATE)/100.0 "
        + "< EXTRACT(MONTH FROM %s)+ EXTRACT(DAY FROM %s)/100.0, 1, 0)"
        % (clauses, clauses)
    )


####################
# Aggregations are a callable on a column expressoin that yields an
# aggregated column expression
# for instance sum(sales) => func.sum(MyTable.sales)
# Different database engines can use different aggregations
####################

aggregations = {
    "sum": func.sum,
    "min": func.min,
    "max": func.max,
    "avg": func.avg,
    "count": func.count,
    "count_distinct": lambda fld: func.count(distinct(fld)),
    # Technically "none" is not an aggregation but we're keeping
    # it here for backward compatibility
    "none": lambda fld: fld,
    None: lambda fld: fld,
    # Percentile aggregations do not work in all engines
    "median": func.median,
    "percentile1": lambda fld: func.percentile_cont(0.01).within_group(fld),
    "percentile5": lambda fld: func.percentile_cont(0.05).within_group(fld),
    "percentile10": lambda fld: func.percentile_cont(0.10).within_group(fld),
    "percentile25": lambda fld: func.percentile_cont(0.25).within_group(fld),
    "percentile50": lambda fld: func.percentile_cont(0.50).within_group(fld),
    "percentile75": lambda fld: func.percentile_cont(0.75).within_group(fld),
    "percentile90": lambda fld: func.percentile_cont(0.90).within_group(fld),
    "percentile95": lambda fld: func.percentile_cont(0.95).within_group(fld),
    "percentile99": lambda fld: func.percentile_cont(0.99).within_group(fld),
}

# Additional aggregations supported on redshift
aggregations_redshift = {
    # Percentile aggregations do not work in all engines
    "median": func.median,
    "percentile1": lambda fld: func.percentile_cont(0.01).within_group(fld),
    "percentile5": lambda fld: func.percentile_cont(0.05).within_group(fld),
    "percentile10": lambda fld: func.percentile_cont(0.10).within_group(fld),
    "percentile25": lambda fld: func.percentile_cont(0.25).within_group(fld),
    "percentile50": lambda fld: func.percentile_cont(0.50).within_group(fld),
    "percentile75": lambda fld: func.percentile_cont(0.75).within_group(fld),
    "percentile90": lambda fld: func.percentile_cont(0.90).within_group(fld),
    "percentile95": lambda fld: func.percentile_cont(0.95).within_group(fld),
    "percentile99": lambda fld: func.percentile_cont(0.99).within_group(fld),
}


aggregations_bigquery = {
    "median": lambda fld: bq_median(fld),
    "percentile1": lambda fld: bq_percentile1(fld),
    "percentile5": lambda fld: bq_percentile5(fld),
    "percentile10": lambda fld: bq_percentile10(fld),
    "percentile25": lambda fld: bq_percentile25(fld),
    "percentile50": lambda fld: bq_median(fld),
    "percentile75": lambda fld: bq_percentile75(fld),
    "percentile90": lambda fld: bq_percentile90(fld),
    "percentile95": lambda fld: bq_percentile95(fld),
    "percentile99": lambda fld: bq_percentile99(fld),
}

# A dictionary of aggregations keyed by sqlalchemy drivername
# 'default' are
aggregations_by_engine = {
    "default": aggregations,
    "redshift+psycopg2": aggregations_redshift,
    "bigquery": aggregations_bigquery,
}


#######################
# Conversions are a callable on a column expression that yields a
# nonaggregated column expression
# for instance, quarter(sales_date) => func.date_trunc('quarter', MyTable.sales_date)
#######################

conversions = {
    "month": lambda fld: func.date_trunc("month", fld),
    "week": lambda fld: func.date_trunc("week", fld),
    "year": lambda fld: func.date_trunc("year", fld),
    "quarter": lambda fld: func.date_trunc("quarter", fld),
    "string": lambda fld: func.cast(fld, String()),
    "int": lambda fld: func.cast(fld, Integer()),
}


conversions_redshift = {
    # age doesn't work on all databases
    "age": lambda fld: postgres_age(fld),
}

conversions_bigquery = {
    "month": lambda fld: func.date_trunc(fld, text("month")),
    "week": lambda fld: func.date_trunc(fld, text("week")),
    "year": lambda fld: func.date_trunc(fld, text("year")),
    "quarter": lambda fld: func.date_trunc(fld, text("quarter")),
    "age": lambda fld: bq_age(fld),
}


# A dictionary of conversions keyed by sqlalchemy drivername
conversions_by_engine = {
    "default": conversions,
    "redshift+psycopg2": conversions_redshift,
    "bigquery": conversions_bigquery,
}
