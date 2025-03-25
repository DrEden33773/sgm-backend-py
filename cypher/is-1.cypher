MATCH
    (person:Person)-[a:isLocatedIn]->(city:City)
WHERE
    person.id = 6597069766660
RETURN person, a, city