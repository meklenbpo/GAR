# GAR prototype

This is an exploration of the GAR model.


## Algorithm

1.  Select all houses from 'houses' that are last in change history:
    - either nextid = 0
    - or isactual = 1
    - check for potential conflicts for either method
    - mind addnum1, addnum2, addtype1, addtype2

2.  Extract required paramteres for the selected houses:
    - postcode number
    - oktmo?

3.  Select historic postcode changes for every actual house.

3.  Traverse the parent object hierarchy through mun_hierarchy
    - find parent object
        - can there be more than one?
        - does it change over time?
        - is it the latest version?
        - develop checks that assert the above three questions
    - collect parent object parameters:
        - name,
        - typename,
        - level
    - verify that parent is actual:
        - isactual = 1
        - enddate = '2079-06-06'



# Issues

- Are there any administrative rayons in the database or can these be
  skipped.