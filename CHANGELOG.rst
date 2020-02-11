
Changelog
=========

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
