 {% macro margin_percent(revenue, purchase_cost) %}
    CASE
        WHEN {{ revenue }} = 0 THEN 0
        ELSE ROUND ((({{ revenue }} - {{ purchase_cost }}) / {{ revenue }}),2) * 100
    END
 {% endmacro %}