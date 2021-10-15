
Changelog
=========

v0.28.0 (2021-10-15)
-----------------------------------------
* Add directives that will convert dates and datetimes to the nearest year/month/day

v0.27.1 (2021-09-14))
-----------------------------------------
* Allow compound selection to take a list of json encoded strings

v0.27.0 (2021-08-26)
-----------------------------------------
* Update requirements
* Drop support for python3.6
* Save metric and dimension keys without deduping

v0.26.1 (2021-07-29)
-----------------------------------------
* Fix aggregation for PaginateInline extension

v0.26.0 (2021-07-15)
-----------------------------------------
* Add PaginateInline extension

v0.25.1 (2021-06-15)
-----------------------------------------
* Fix datatype tracking in some cases

v0.25.0 (2021-06-07)
-----------------------------------------
* Add to date syntax
* Avoid installing a top-level tests package in setup.py

v0.24.1 (2021-06-10)
-----------------------------------------
* Fix datatype tracking in some cases

v0.24.0 (2021-05-14)
-----------------------------------------
* Track the datatype used by ingredient columns
* Require parsed metrics to generate a number

v0.23.4 (2021-05-03)
-----------------------------------------
* Improve automatic filtering with uncompilable ingredients

v0.23.3 (2021-04-29)
-----------------------------------------
* Fix column_type for timestamps

v0.23.2 (2021-02-09)
-----------------------------------------
* Apply a default ordering when paginating

v0.23.1 (2021-02-08)
-----------------------------------------
* Fix sql generation of timestamp truncated columns in bigquery

v0.23.0 (2021-02-01)
-----------------------------------------
* Improve the lark parser to validate explicitly using the database columns and
  column types available in the data.
* Run a validation phase on a parsed tree to make sure that arguments are correct types.
* Return descriptive errors
* Improve cross database support

v0.22.1 (2020-12-23)
-----------------------------------------
* Like and ilike filter generation is more lenient

v0.22.0 (2020-12-10)
-----------------------------------------
* Drop python2 support

v0.21.0 (2020-10-20)
-----------------------------------------
* Add [syntax] to disambiguate database columns in parsed fields
* Save original config to ingredient when generating parsed fields.

v0.20.1 (2020-10-07)
-----------------------------------------
* Fix issue with parsing >= and <=

v0.20.0 (2020-10-02)
-----------------------------------------
* Update total_count to use caching
* Fix datatime auto conversions

0.19.1 (2020-09-10)
-----------------------------------------
* Drop python2.7 testing support (Python2.7 support will be dropped in 0.20)
* Improve type identification in Ingredient.build_filter

0.19.0 (2020-09-04)
-----------------------------------------
* Support and documentation for compound selection in automatic filters
* Support for different sqlalchemy generation when using parsed fields
* Add support for date conversions and percentiles in bigquery.
* Ingredient.build_filters now returns SQLAlchemy BinaryExpression rather than Filter objects.

0.18.1 (2020-08-07)
-----------------------------------------
* Fix a bug in filter binning
* Happy birthday, Zoe!

0.18.0 (2020-07-31)
-----------------------------------------
* Add automatic filter binning for redshift to reduce required query compilations
* Add parsed field converters to perform casting and date truncation.

0.17.2 (2020-07-21)
-----------------------------------------
* Fix Paginate search to use value roles

0.17.1 (2020-07-09)
-----------------------------------------
* Fix parsed syntax for `field IS NULL`

0.17.0 (2020-06-26)
-----------------------------------------
* Set bucket default label to "Not found"
* Use sureberus to validate lookup is a dictionary if present in Dimension config
* Fix to ensure pagination page is 1 even if there is no data
* On shelf construction, create InvalidIngredient for ingredients that fail construction

0.16.0 (2020-06-19)
-----------------------------------------
* Ignore order_by on a recipe if the ingredient has not been added to the dimensions or metrics.
* Allows case insensitivity in "kind:" and support "kind: Measure" as an alternative to "kind: Metric"
* Fix like/ilike and pagination_q filtering against dimensions that have a non-string ID.
* Fix parsed sql generation for AND and OR
* Fix parsed sql generation for division when one of the terms is a constant (like sum(people) / 100.0)
* Adds IS NULL as a boolean expression 
* Adds "Intelligent date" calculations to allow more useful date calculations relative to current date

0.15.0 (2020-05-08)
-----------------------------------------
* Ignore order_by if ingredients have not been added
* Support measure as a synonym for metric and be lenient about capitalization
  in shelf config

0.14.0 (2020-03-06)
-----------------------------------------
* Support graceful ingredient failures when ingredients can not be constructed from config.

0.13.1 (2020-02-11)
-----------------------------------------
* Fix a pg8000 issue

0.13.0 (2020-01-28)
-----------------------------------------

* Extend grouping strategies so recipes can also order by column labels
* Create a new shelf configuration that uses lark to parse text into SQLAlchemy.

0.12.0 (2019-11-25)
-----------------------------------------

* remove flapjack_stack and pyhash dependencies
* Add percentile aggregations to metrics from config.
* Use more accurate fetched_from_cache caching query attribute
* Add grouping strategies so recipes can group by column labels

0.11.0 (2019-11-07)
-----------------------------------------
* Add Paginate extension
* Fix deterministic Anonymization in python3
* CI improvements

0.10.0 (2019-08-07)
-----------------------------------------
* Support multiple quickselects which are ORed together

0.9.0 (2019-08-07)
-----------------------------------------
* Replace quickfilter with quickselect
* Improve and publish docs on at recipe.readthedocs.io
* Happy birthday, Zoe!

0.8.0 (2019-07-08)
-----------------------------------------
* Add cache control options.

0.7.0 (2019-06-24)
-----------------------------------------

* Support date ranges in configuration defined ingredients
* Add like, ilike, between in ingredients defined from config
* Better handling in automatic filters when Nones appear in lists
* Remove dirty flag
* Ingredients defined from config support safe division by default
* [ISSUE-37] Allow Dimension defined from config to be defined using buckets

0.6.2 (2019-06-11)
-----------------------------------------


0.1.0 (2017-02-05)
-----------------------------------------

* First release on PyPI.
