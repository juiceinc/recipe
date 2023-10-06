from recipe import BadRecipe
from recipe.extensions import BlendRecipe, CompareRecipe
from tests.test_base import RecipeTestCase
from recipe.exceptions import BadRecipe


class CompareRecipeTestCase(RecipeTestCase):
    extension_classes = [CompareRecipe]

    def setUp(self):
        super().setUp()
        self.shelf = self.census_shelf

    def test_compare(self):
        """A basic comparison recipe. The base recipe looks at all data, the
        comparison only applies to vermont

        Note: Ordering is only preserved on postgres engines.
        """

        r = self.recipe().metrics("pop2000").dimensions("sex").order_by("sex")
        r = r.compare(
            self.recipe()
            .metrics("pop2000")
            .dimensions("sex")
            .filters(self.census_table.c.state == "Vermont")
        )

        self.assertRecipeSQL(
            r,
            """SELECT census.sex AS sex,
       sum(census.pop2000) AS pop2000,
       avg(anon_1.pop2000) AS pop2000_compare
FROM census
LEFT OUTER JOIN
  (SELECT census.sex AS sex,
          sum(census.pop2000) AS pop2000
   FROM census
   WHERE census.state = 'Vermont'
   GROUP BY sex) AS anon_1 ON census.sex = anon_1.sex
GROUP BY census.sex
ORDER BY census.sex""",
        )
        self.assertRecipeCSV(
            r,
            """
        sex,pop2000,pop2000_compare,sex_id
        F,3234901,310948.0,F
        M,3059809,298532.0,M
        """,
        )

    def test_compare_custom_aggregation(self):
        """A basic comparison recipe. The base recipe looks at all data, the
        comparison only applies to vermont

        Note: Ordering is only preserved on postgres engines.
        """
        r = self.recipe().metrics("pop2000").dimensions("sex").order_by("sex")
        r = r.compare(
            self.recipe()
            .metrics("pop2000_sum")
            .dimensions("sex")
            .filters(self.census_table.c.state == "Vermont")
        )

        self.assertRecipeSQL(
            r,
            """SELECT census.sex AS sex,
       sum(census.pop2000) AS pop2000,
       sum(anon_1.pop2000_sum) AS pop2000_sum_compare
FROM census
LEFT OUTER JOIN
  (SELECT census.sex AS sex,
          sum(census.pop2000) AS pop2000_sum
   FROM census
   WHERE census.state = 'Vermont'
   GROUP BY sex) AS anon_1 ON census.sex = anon_1.sex
GROUP BY census.sex
ORDER BY census.sex""",
        )
        self.assertRecipeCSV(
            r,
            """
            sex,pop2000,pop2000_sum_compare,sex_id
            F,3234901,53483056,F
            M,3059809,51347504,M
            """,
        )

    def test_compare_suffix(self):
        """Test that the proper suffix gets added to the comparison metrics"""

        r = self.recipe().metrics("pop2000").dimensions("sex").order_by("sex")
        r = r.compare(
            self.recipe()
            .metrics("pop2000")
            .dimensions("sex")
            .filters(self.census_table.c.state == "Vermont"),
            suffix="_x",
        )

        self.assertRecipeSQL(
            r,
            """SELECT census.sex AS sex,
       sum(census.pop2000) AS pop2000,
       avg(anon_1.pop2000) AS pop2000_x
FROM census
LEFT OUTER JOIN
  (SELECT census.sex AS sex,
          sum(census.pop2000) AS pop2000
   FROM census
   WHERE census.state = 'Vermont'
   GROUP BY sex) AS anon_1 ON census.sex = anon_1.sex
GROUP BY census.sex
ORDER BY census.sex""",
        )
        self.assertRecipeCSV(
            r,
            """
            sex,pop2000,pop2000_x,sex_id
            F,3234901,310948.0,F
            M,3059809,298532.0,M
            """,
        )

    def test_mismatched_dimensions_raises(self):
        """Dimensions in the comparison recipe must be a subset of the
        dimensions in the base recipe"""
        r = self.recipe().metrics("pop2000").dimensions("sex").order_by("sex")
        r = r.compare(
            self.recipe()
            .metrics("pop2000")
            .dimensions("state")
            .filters(self.census_table.c.state == "Vermont"),
            suffix="_x",
        )

        with self.assertRaises(BadRecipe):
            r.all()


class BlendRecipeTestCase(RecipeTestCase):
    extension_classes = [BlendRecipe]

    def setUp(self):
        super().setUp()
        self.shelf = self.census_shelf

    def test_self_blend(self):
        """A basic comparison recipe. The base recipe looks at all data, the
        comparison only applies to vermont

        Note: Ordering is only preserved on postgres engines.
        """

        r = self.recipe().metrics("pop2000").dimensions("sex").order_by("sex")

        blend_recipe = (
            self.recipe()
            .metrics("pop2008")
            .dimensions("sex")
            .filters(self.census_table.c.sex == "F")
        )
        r = r.full_blend(blend_recipe, join_base="sex", join_blend="sex")

        self.assertRecipeSQL(
            r,
            """SELECT census.sex AS sex,
       sum(census.pop2000) AS pop2000,
       anon_1.pop2008 AS pop2008
FROM census
LEFT OUTER JOIN
  (SELECT census.sex AS sex,
          sum(census.pop2008) AS pop2008
   FROM census
   WHERE census.sex = 'F'
   GROUP BY sex) AS anon_1 ON census.sex = anon_1.sex
GROUP BY census.sex
ORDER BY census.sex""",
        )
        self.assertRecipeCSV(
            r,
            """
            sex,pop2000,pop2008,sex_id
            F,3234901,3499762,F
            M,3059809,,M
            """,
        )

    def test_blend(self):
        """A basic comparison recipe. The base recipe looks at all data, the
        comparison only applies to vermont

        Note: Ordering is only preserved on postgres engines.
        """

        r = self.recipe().metrics("pop2000").dimensions("state").order_by("state")

        blend_recipe = (
            self.recipe()
            .shelf(self.statefact_shelf)
            .dimensions("state", "abbreviation")
        )
        r = r.blend(blend_recipe, join_base="state", join_blend="state")

        self.assertRecipeSQL(
            r,
            """SELECT census.state AS state,
       sum(census.pop2000) AS pop2000,
       anon_1.abbreviation AS abbreviation
FROM census
JOIN
  (SELECT state_fact.abbreviation AS abbreviation,
          state_fact.name AS state
   FROM state_fact
   GROUP BY abbreviation,
            state) AS anon_1 ON census.state = anon_1.state
GROUP BY census.state,
         anon_1.abbreviation
ORDER BY census.state""",
        )
        self.assertRecipeCSV(
            r,
            """
            state,pop2000,abbreviation,abbreviation_id,state_id
            Tennessee,5685230,TN,TN,Tennessee
            Vermont,609480,VT,VT,Vermont
            """,
        )
