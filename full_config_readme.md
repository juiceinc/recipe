Shelves are defined in yaml

They consist of a name and a definition


    dim1
        field: [department] + [foo]
        singular: moo
        plural: moos
    met1
        field: sum(count_unique([department]))
        singular: cow
        plural: cows
        format: .2f
    
Recipes can be defined by config

    shelf:
        - Shelf definition
    dimensions List[str, dict]:
        - dim1
    metrics List[str, dict]:
        - met1
    havings List[str, dict]:
    filters List[str, dict]:
    automatic_filters [dict, list[dict]]:
        dim1: 
            - Radiology
            - Nursing
        met1__gt
    variables [dict]:
        dict
    meta:
        connection info:
        grammar [str]:
    
Automatic filters can be a list or a dict. Filter syntax 

    {ingredient}__{operator}


Constants

Variables



Joins












### Kinds of Ingredients

Ingredients always require a `kind`. It can be one of

- Dimension
- IdValueDimension
- Metric
- DivideMetric
- WtdAvgMetric
- ConditionalMetric

Ingredients may require one or two fields. Fields look like this

    # This is a field using the default table
    field: moo
    # MyTable.moo

    # This is the same as above
    # MyTable.moo
    field:
        value: moo

    # This is also the same as above
    # MyTable.moo
    field:
        value: moo
        aggregation: none

    # Fields can also define conditions that apply inside of the
    # aggregation
    # case when MyTable.sales_id in (1,2,3) then MyTable.moo end
    field:
        value: moo
        condition:
            field: sales_id
            in: [1,2,3]

    # Aggregations can exits
    # func.sum(MyTable.moo)
    field:
        value: moo
        aggregation: sum

    # Conditions work inside of the aggregation
    # func.max(case when MyTable.sales_id in (1,2,3) then MyTable.moo end)
    field:
        value: moo
        aggregation: max
        condition:
            field: sales_id
            in: [1,2,3]

Potential aggregations are

- sum
- min
- max
- avg
- count
- count_distinct

#### Dimension

field is required

    foo:
        kind: Dimension
        field: moo

You can also create lookups.

    foo:
        kind: LookupDimension
        field: moo
        lookup_default: Unknown
        lookup:
            NY: New York
            VT: Vermont
            GA: Georgia

This creates

    'foo': Dimension(MyTable.moo,
                           lookup={
                             'NY': 'New York',
                             'VT': 'Vermont',
                             'GA': 'Georgia'
                           },
                           lookup_default='Unknown')

#### IdValueDimension

field is required.
id_field is based on field+'_id' if not provided

    foo:
        kind: IdValueDimension
        field: moo
        id_field: moo_id

This creates

    'foo': IdValueDimension(MyTable.moo, MyTable.moo_id)

#### Metric

field is required. All metric fields are aggregated using `sum` by default


A field can take a string or an object.

    foo:
        kind: Metric
        field: foo

This creates

    'foo': Metric(func.sum(foo)),


aggregation is optional

    foo:
        kind: Metric
        field:
            value: foo
            aggregation: max

This creates

    'foo': Metric(func.max(MyTable.foo)),

You can define a conditional field that operates on the aggregation

    foo:
        kind: Metric
        field:
            value: sales
            condition:
                field: sale_date
                range: last_year

This creates

    'foo': Metric(func.sum(
      case when MyTable.sale_date between
        cur_date() - '1 year' and cur_date() then MyTable.sales))


Here is another example of a condition.

    foo:
        kind: Metric
        field:
            value: sales
            condition:
                field: sale_date
                gt: 2015-01-01

This creates

    'foo': Metric(func.sum(
      case when MyTable.sale_date > '2015-01-01' then MyTable.sales))

#### DivideMetric

numerator_field and denominator_field are required.

    foo:
        kind: DivideMetric
        numerator_field: sales
        denominator_field: count(distinct(sales))

How to define the important pct teenage calculation

    pct_teen:
        kind: DivideMetric
        numerator_field:
            value: person_id
            aggregation: count_distinct
            condition:
                field: age
                between: [13, 19]
        denominator_field:
            value: person_id
            aggregation: count_distinct

How to define the important average price of product sold

    avg_price:
        kind: DivideMetric
        numerator_field: sales
        denominator_field: quantity




#### WtdAvgMetric

#### Filter

#### Having
from recipe.ingredients import Ingredient, Dimension, \
    IdValueDimension, Metric, DivideMetric, WtdAvgMetric,
    Filter, Having
