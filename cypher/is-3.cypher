MATCH
    (person:Person)-[a:knows]->(friend:Person)
WHERE
    person.id <> friend.id AND
    person.id = 6597069766660
RETURN person, a, friend
// LIMIT 10