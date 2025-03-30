MATCH
    (c: Person)-[_a: knows]->(a: Person),
    (c)-[_b: isLocatedIn]->(city_c: City),
    (c)-[_c: knows]->(b: Person),
    (a)-[_d: knows]->(b),
    (a)-[_e: isLocatedIn]->(city_a: City),
    (b)-[_f: isLocatedIn]->(city_b: City),
    (city_a)-[_g: isPartOf]->(country: Country),
    (city_b)-[_h: isPartOf]->(country),
    (city_c)-[_i: isPartOf]->(country)
WHERE
    country.name = 'China'
RETURN 
    c,
    a,
    b,
    city_c,
    city_a,
    city_b,
    country,
    _a,
    _b,
    _c,
    _d,
    _e,
    _f,
    _g,
    _h,
    _i