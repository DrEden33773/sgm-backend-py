MATCH
    (person:Person)-[a:knows]->(otherPerson:Person),
    (friend:Person)-[b:knows]->(otherPerson),
    (post:Post)-[c:hasCreator]->(person),
    (otherPost:Post)-[d:hasCreator]->(friend),
    (post)-[e:hasTag]->(tag:Tag),
    (otherPost)-[f:hasTag]->(tag)
WHERE
    person.id <> otherPerson.id AND
    friend.id <> otherPerson.id AND
    otherPerson.id = 246
RETURN
    person, a, otherPerson, b, friend, c, post, d, otherPost, e, f, tag
// LIMIT 10