MATCH
    (person:Person)-[a:knows]->(friend:Person),
    (comment:Comment)-[b:hasCreator]->(friend),
    (comment)-[c:replyOf]->(post:Post),
    (post)-[d:hasTag]->(tag:Tag),
    (tag)-[e:hasType]->(tagClass:Tagclass),
    (tagClass)-[f:isSubclassOf]->(otherTagClass:Tagclass)
WHERE
    person.id <> friend.id AND
    person.id = 8796093022390 AND
    otherTagClass.name = 'Artist'
RETURN
    person, a, friend, b, comment, c, post, d, tag, e, tagClass, f, otherTagClass
// LIMIT 10