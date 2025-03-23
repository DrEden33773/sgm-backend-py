MATCH
    (post:Post)-[a:hasTag]->(tag:Tag),
    (post)-[b:hasTag]->(otherTag:Tag),
    (post)-[c:hasCreator]->(otherPerson:Person),
    (person:Person)-[d:knows]->(otherPerson)
WHERE
    tag <> otherTag AND
    tag.name <> otherTag.name AND
    tag.name = "Joan_Crawford" AND
    person.id = 153
RETURN post, a, b, c, tag, otherTag, person, d, otherPerson
// LIMIT 10