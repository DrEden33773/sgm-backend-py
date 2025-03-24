MATCH
    (person:Person)-[a:knows]->(otherPerson:Person),
    (forum:Forum)-[b:hasMember]->(otherPerson),
    (forum)-[c:containerOf]->(post:Post),
    (post)-[d:hasCreator]->(otherPerson)
WHERE
    person.id <> otherPerson.id AND
    person.id = 94
RETURN
    person, a, otherPerson, b, forum, c, post, d
// LIMIT 10