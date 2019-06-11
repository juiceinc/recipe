"""
Test recipes built from yaml files in the ingredients directory.

"""

import os

import pytest
from tests.test_base import Census, MyTable, oven

from recipe import AutomaticFilters, BadIngredient, Recipe, Shelf


class TestRecipeIngredientsYaml(object):

    def setup(self):
        self.session = oven.Session()

    def assert_recipe_csv(self, recipe, csv_text):
        assert recipe.dataset.export('csv', lineterminator='\n') == csv_text

    def validated_shelf(self, shelf_name, table):
        """Load a file from the sample ingredients.yaml files."""
        d = os.path.dirname(os.path.realpath(__file__))
        fn = os.path.join(d, 'ingredients', shelf_name)
        contents = open(fn).read()
        return Shelf.from_validated_yaml(contents, table)

    def unvalidated_shelf(self, shelf_name, table):
        """Load a file from the sample ingredients.yaml files."""
        d = os.path.dirname(os.path.realpath(__file__))
        fn = os.path.join(d, 'ingredients', shelf_name)
        contents = open(fn).read()
        return Shelf.from_yaml(contents, table)

    def test_ingredients1_from_validated_yaml(self):
        shelf = self.validated_shelf('ingredients1.yaml', MyTable)
        recipe = Recipe(
            shelf=shelf, session=self.session
        ).metrics('age').dimensions('first')
        self.assert_recipe_csv(recipe, '''first,age,first_id
hi,15,hi
''')

    def test_ingredients1_from_yaml(self):
        shelf = self.validated_shelf('ingredients1.yaml', MyTable)
        recipe = Recipe(
            shelf=shelf, session=self.session
        ).metrics('age').dimensions('first')
        self.assert_recipe_csv(recipe, '''first,age,first_id
hi,15,hi
''')

    def test_census_from_validated_yaml(self):
        shelf = self.validated_shelf('census.yaml', Census)
        recipe = Recipe(
            shelf=shelf, session=self.session
        ).dimensions('state').metrics('pop2000', 'ttlpop').order_by('state')
        self.assert_recipe_csv(
            recipe, '''state,pop2000,ttlpop,state_id
Tennessee,5685230,11887637,Tennessee
Vermont,609480,1230082,Vermont
'''
        )

    def test_census_from_yaml(self):
        shelf = self.unvalidated_shelf('census.yaml', Census)
        recipe = Recipe(
            shelf=shelf, session=self.session
        ).dimensions('state').metrics('pop2000', 'ttlpop').order_by('state')
        self.assert_recipe_csv(
            recipe, '''state,pop2000,ttlpop,state_id
Tennessee,5685230,11887637,Tennessee
Vermont,609480,1230082,Vermont
'''
        )

    def test_nested_census_from_validated_yaml(self):
        """Build a recipe that depends on the results of another recipe """
        shelf = self.validated_shelf('census.yaml', Census)
        recipe = Recipe(
            shelf=shelf, session=self.session
        ).dimensions('state').metrics('pop2000', 'ttlpop').order_by('state')
        nested_shelf = self.validated_shelf('census_nested.yaml', recipe)
        nested_recipe = Recipe(
            shelf=nested_shelf, session=self.session
        ).metrics('ttlpop', 'num_states')
        assert nested_recipe.to_sql(
        ) == '''SELECT count(anon_1.state) AS num_states,
       sum(anon_1.ttlpop) AS ttlpop
FROM
  (SELECT census.state AS state,
          sum(census.pop2000) AS pop2000,
          sum(census.pop2000 + census.pop2008) AS ttlpop
   FROM census
   GROUP BY census.state
   ORDER BY census.state) AS anon_1'''
        self.assert_recipe_csv(
            nested_recipe, '''num_states,ttlpop
2,13117719
'''
        )

    def test_nested_census_from_yaml(self):
        shelf = self.unvalidated_shelf('census.yaml', Census)
        recipe = Recipe(
            shelf=shelf, session=self.session
        ).dimensions('state').metrics('pop2000', 'ttlpop').order_by('state')
        nested_shelf = self.unvalidated_shelf('census_nested.yaml', recipe)
        nested_recipe = Recipe(
            shelf=nested_shelf, session=self.session
        ).metrics('ttlpop', 'num_states')
        self.assert_recipe_csv(
            nested_recipe, '''num_states,ttlpop
2,13117719
'''
        )

    def test_complex_census_from_validated_yaml(self):
        """Build a recipe that uses complex definitions dimensions and
        metrics """
        shelf = self.validated_shelf('census_complex.yaml', Census)
        recipe = Recipe(
            shelf=shelf, session=self.session
        ).dimensions('state').metrics('pop2000').order_by('state')
        assert recipe.to_sql() == '''SELECT census.state AS state_raw,
       sum(CASE
               WHEN (census.age > 40) THEN census.pop2000
           END) AS pop2000
FROM census
GROUP BY census.state
ORDER BY census.state'''
        self.assert_recipe_csv(
            recipe, '''state_raw,pop2000,state,state_id
Tennessee,2392122,The Volunteer State,Tennessee
Vermont,271469,The Green Mountain State,Vermont
'''
        )

    def test_complex_census_from_validated_yaml_math(self):
        """Build a recipe that uses complex definitions dimensions and
        metrics """
        shelf = self.validated_shelf('census_complex.yaml', Census)
        recipe = Recipe(
            shelf=shelf, session=self.session
        ).dimensions('state').metrics('allthemath')
        assert recipe.to_sql() == '''SELECT census.state AS state_raw,
       sum((((census.pop2000 + census.pop2008) - census.pop2000) * census.pop2008) / (coalesce(CAST(census.pop2000 AS FLOAT), 0.0) + 1e-09)) AS allthemath
FROM census
GROUP BY census.state'''  # noqa: E501

    def test_complex_census_quickfilter_from_validated_yaml(self):
        """Build a recipe that uses complex definitions dimensions and
        metrics and quickfilters"""
        shelf = self.validated_shelf('census_complex.yaml', Census)
        recipe = Recipe(
            shelf=shelf,
            session=self.session,
            extension_classes=(AutomaticFilters,)
        ).dimensions('state').metrics('pop2008'
                                     ).order_by('state').automatic_filters({
                                         'state__quickfilter': 'younger'
                                     })
        assert recipe.to_sql() == '''SELECT census.state AS state_raw,
       sum(census.pop2008) AS pop2008
FROM census
WHERE census.age < 40
GROUP BY census.state
ORDER BY census.state'''
        self.assert_recipe_csv(
            recipe, '''state_raw,pop2008,state,state_id
Tennessee,3297299,The Volunteer State,Tennessee
Vermont,300605,The Green Mountain State,Vermont
'''
        )
        recipe = Recipe(
            shelf=shelf,
            session=self.session,
            extension_classes=(AutomaticFilters,)
        ).dimensions('state').metrics('pop2008'
                                     ).order_by('state').automatic_filters({
                                         'state__quickfilter': 'vermontier'
                                     })
        assert recipe.to_sql() == '''SELECT census.state AS state_raw,
       sum(census.pop2008) AS pop2008
FROM census
WHERE census.state = 'Vermont'
GROUP BY census.state
ORDER BY census.state'''
        self.assert_recipe_csv(
            recipe, '''state_raw,pop2008,state,state_id
Vermont,620602,The Green Mountain State,Vermont
'''
        )

    def test_shelf_with_references(self):
        """Build a recipe using a shelf that uses field references """
        shelf = self.validated_shelf('census_references.yaml', Census)
        recipe = Recipe(shelf=shelf, session=self.session).\
            dimensions('state').metrics('popdivide').order_by('state')
        assert recipe.to_sql() == '''SELECT census.state AS state_raw,
       CAST(sum(CASE
                    WHEN (census.age > 40) THEN census.pop2000
                END) AS FLOAT) / (coalesce(CAST(sum(census.pop2008) AS FLOAT), 0.0) + 1e-09) AS popdivide
FROM census
GROUP BY census.state
ORDER BY census.state'''  # noqa: E501
        self.assert_recipe_csv(
            recipe, '''state_raw,popdivide,state,state_id
Tennessee,0.3856763995010324,The Volunteer State,Tennessee
Vermont,0.4374284968466095,The Green Mountain State,Vermont
'''
        )

    def test_shelf_with_condition_references(self):
        """Build a recipe using a shelf that uses condition references """
        shelf = self.validated_shelf('census_references.yaml', Census)
        recipe = Recipe(
            shelf=shelf, session=self.session
        ).dimensions('state').metrics('pop2008oldsters').order_by('state')
        assert recipe.to_sql() == '''SELECT census.state AS state_raw,
       sum(CASE
               WHEN (census.age > 40) THEN census.pop2008
           END) AS pop2008oldsters
FROM census
GROUP BY census.state
ORDER BY census.state'''  # noqa: E501
        self.assert_recipe_csv(
            recipe, '''state_raw,pop2008oldsters,state,state_id
Tennessee,2821955,The Volunteer State,Tennessee
Vermont,311842,The Green Mountain State,Vermont
'''
        )

    def test_bad_census_from_validated_yaml(self):
        """ Test a bad yaml file """
        with pytest.raises(Exception):
            self.validated_shelf('census_bad.yaml', Census)

    def test_bad_census_from_yaml(self):
        """ Test a bad yaml file """
        with pytest.raises(BadIngredient):
            self.unvalidated_shelf('census_bad.yaml', Census)

    def test_bad_census_in_from_validated_yaml(self):
        """ Test a bad yaml file """
        with pytest.raises(Exception):
            self.validated_shelf('census_bad_in.yaml', Census)

    def test_bad_census_in_from_yaml(self):
        """ Test a bad yaml file """
        with pytest.raises(BadIngredient):
            self.unvalidated_shelf('census_bad_in.yaml', Census)

    def test_deprecated_ingredients_dividemetric(self):
        """ Test deprecated ingredient kinds in a yaml file """
        shelf = self.validated_shelf('census_deprecated.yaml', Census)

        # We can DivideMetric
        recipe = Recipe(
            shelf=shelf, session=self.session
        ).dimensions('state').metrics('popchg').order_by('state')
        assert recipe.to_sql() == '''SELECT census.state AS state,
       CAST(sum(census.pop2000) AS FLOAT) / (coalesce(CAST(sum(census.pop2008) AS FLOAT), 0.0) + 1e-09) AS popchg
FROM census
GROUP BY census.state
ORDER BY census.state'''  # noqa: E501
        self.assert_recipe_csv(
            recipe, '''state,popchg,state_id
Tennessee,0.9166167263773563,Tennessee
Vermont,0.9820786913351858,Vermont
'''
        )

    def test_deprecated_ingredients_lookupdimension(self):
        """ Test deprecated ingredient kinds in a yaml file """
        shelf = self.validated_shelf('census_deprecated.yaml', Census)

        # We can LookupDimension
        recipe = Recipe(
            shelf=shelf, session=self.session
        ).dimensions('state_characteristic'
                    ).metrics('pop2000').order_by('state_characteristic')
        assert recipe.to_sql() == '''SELECT census.state AS state_characteristic_raw,
       sum(census.pop2000) AS pop2000
FROM census
GROUP BY census.state
ORDER BY census.state'''  # noqa: E501
        self.assert_recipe_csv(
            recipe,
            '''state_characteristic_raw,pop2000,state_characteristic,state_characteristic_id
Tennessee,5685230,Volunteery,Tennessee
Vermont,609480,Taciturny,Vermont
'''
        )

    def test_deprecated_ingredients_idvaluedimension(self):
        """ Test deprecated ingredient kinds in a yaml file """
        shelf = self.validated_shelf('census_deprecated.yaml', Census)

        # We can IdValueDimension
        recipe = Recipe(
            shelf=shelf, session=self.session
        ).dimensions('state_idval').metrics('pop2000').order_by('state_idval'
                                                               ).limit(5)
        assert recipe.to_sql() == '''SELECT census.pop2000 AS state_idval_id,
       census.state AS state_idval,
       sum(census.pop2000) AS pop2000
FROM census
GROUP BY census.pop2000,
         census.state
ORDER BY census.state,
         census.pop2000 LIMIT 5
OFFSET 0'''  # noqa: E501
        self.assert_recipe_csv(
            recipe, '''state_idval_id,state_idval,pop2000,state_idval_id
5033,Tennessee,5033,5033
5562,Tennessee,5562,5562
6452,Tennessee,6452,6452
7322,Tennessee,7322,7322
8598,Tennessee,8598,8598
'''
        )

    def test_deprecated_ingredients_wtdavgmetric(self):
        """ Test deprecated ingredient kinds in a yaml file """
        shelf = self.validated_shelf('census_deprecated.yaml', Census)

        # We can IdValueDimension
        recipe = Recipe(
            shelf=shelf, session=self.session
        ).dimensions('state').metrics('avgage').order_by('state_idval')
        assert recipe.to_sql() == '''SELECT census.state AS state,
       CAST(sum(census.age * census.pop2000) AS FLOAT) / (coalesce(CAST(sum(census.pop2000) AS FLOAT), 0.0) + 1e-09) AS avgage
FROM census
GROUP BY census.state
ORDER BY census.state,
         census.pop2000'''  # noqa: E501

        self.assert_recipe_csv(
            recipe, '''state,avgage,state_id
Tennessee,36.24667550829078,Tennessee
Vermont,37.0597968760254,Vermont
'''
        )
